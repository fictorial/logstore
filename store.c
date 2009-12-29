#include "store.h"
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <sys/mman.h>
#include <pthread.h>

#ifdef O_NOATIME
#define OTHER_OPEN_FLAGS O_NOATIME
#else
#define OTHER_OPEN_FLAGS 0
#endif

// See open(2) and EINTR. 

#define retry(expr) ({ \
    int __rc; \
    do __rc = (expr); \
    while (__rc == -1 && errno == EINTR); __rc; })

// Index file entries are 64-bit integers.

#define IENTRY_SZ       8

// This index file growth factor is arbitrary.  The idea is that the index file
// is often going to be memory-mapped for performance reasons, but
// memory-mapped files cannot grow through a memory-map.  Thus, we treat the
// index file as a sparse file and grow it by seeking+writing beyond the EOF by
// a certain amount and remapping the file.

#define IFILE_GROW_BY   10000

// A store is a log file and an index file (<path>-index)

int store_open(struct store *s, const char *path) {
    if (!s || !path) 
        return STORE_EINVAL;

    // Open log file.

    if (-1 == (s->lfd = open(path, O_CREAT|O_APPEND|O_RDWR|OTHER_OPEN_FLAGS, 0777))) {
        perror("open log file");
        return STORE_EIO;
    }

    // Get size of log file.

    struct stat st;
    if (fstat(s->lfd, &st) < 0 || !S_ISREG(st.st_mode)) {
        perror("open log file");
        close(s->lfd);
        return STORE_EIO;
    }
    s->lsz = st.st_size;

    // Open index file.

    char *ipath = malloc(strlen(path) + strlen("-index") + 1);
    if (!ipath) return STORE_ENOMEM;
    sprintf(ipath, "%s-index", path);
    s->ifd = open(ipath, O_CREAT|O_RDWR|OTHER_OPEN_FLAGS, 0777);
    free(ipath);
    if (-1 == s->ifd) {
        perror("open index file");
        close(s->lfd);
        return STORE_EIO;
    }

    // Determine the capacity of the index file.

    struct stat ist;
    if (fstat(s->ifd, &ist) < 0) {
        perror("index file fstat");
        close(s->ifd);
        close(s->lfd);
        return STORE_EIO;
    }
    s->icap = ist.st_size / IENTRY_SZ;

    // If needed, grow the (sparse) index file to hold a decent number of
    // entries for mmap.

    s->igrowths = 0;

    if (s->icap == 0) {
        char zero = 0;
        off_t ofs = IFILE_GROW_BY*IENTRY_SZ - sizeof(char);
        if (retry(pwrite(s->ifd, &zero, sizeof(char), ofs)) < sizeof(char)) {
            perror("index file growth");
            close(s->ifd);
            close(s->lfd);
            return STORE_EIO;
        }
        s->icap = IFILE_GROW_BY;
        s->igrowths++;
    }

    // Get number of entries in the store from the beginning of the index file.

    if (retry(read(s->ifd, &s->icount, 8)) < 8) {
        perror("read entry count");
        close(s->lfd);
        close(s->ifd);
        return STORE_EIO;
    }

    // Try to mmap the index file; falls back to regular file i/o on failure.

    s->imm = mmap(0, s->icap * IENTRY_SZ, PROT_READ | PROT_WRITE, MAP_SHARED, s->ifd, 0);
    if (MAP_FAILED == s->imm) {
        perror("mmap index file");
        s->imm = NULL;
        s->imm_sz = 0;
    } else {
        s->imm_sz = s->icap * IENTRY_SZ;
    }

    pthread_mutex_init(&s->mutex, NULL);

    return STORE_OK;
}

int store_genid(struct store *s, struct stored *v) {
    if (!s || !v) 
        return STORE_EINVAL;

    pthread_mutex_lock(&s->mutex);

    v->id = s->icount++;

    // Save the number of used index entries in the index file (at offset 0).

    if (s->imm && s->imm_sz) {
        *(uint64_t *)s->imm = s->icount;
    } else if (retry(pwrite(s->ifd, &s->icount, 8, 0)) < 8) {
        perror("write next id");
        pthread_mutex_unlock(&s->mutex);
        return STORE_EIO;
    }

    // If the index file is too big and we're using mmap to access its content,
    // unmap, grow the file, and remap.

    if (s->icount == s->icap && s->imm && s->imm_sz) {
        munmap(s->imm, s->imm_sz);

        s->icap += IFILE_GROW_BY;
        off_t new_sz = s->icap * IENTRY_SZ;

        char zero = 0;
        if (retry(pwrite(s->ifd, &zero, sizeof(char), new_sz - sizeof(char))) < sizeof(char)) {
            perror("grow index file");
            pthread_mutex_unlock(&s->mutex);
            return STORE_EIO;
        }

        s->igrowths++;

        s->imm = mmap(0, new_sz, PROT_READ | PROT_WRITE, MAP_SHARED, s->ifd, 0);
        if (MAP_FAILED == s->imm) {
            perror("re-mmap index file");
            s->imm = NULL;
            s->imm_sz = 0;
        } else {
            s->imm_sz = new_sz;
        }
    }
    
    pthread_mutex_unlock(&s->mutex);
    return STORE_OK;
}

// index entries are 64-bit numbers with high 16 bits for revision, low 48 bits for log offset.
// max revisions: ~65K; max log file size: ~260GiB
// an index file is a sparse file wherein the entry for id X is stored at byte offset X*8.

// Get the log-file offset given an index file entry.

static inline uint64_t ientry_ofs(uint64_t e) {
    return e & 0x0000ffffffffffff;
}

// Get the revision of a given index file entry.

static inline uint16_t ientry_rev(uint64_t e) {
    return (e & 0xffff000000000000) >> 48;
}

// Make an index file entry (offset and revision).

static inline uint64_t ientry_make(uint64_t ofs, uint16_t rev) {
    uint64_t e = rev;
    e <<= 48;
    e |= (ofs & 0x0000ffffffffffff);
    return e;
}

// The first 8 bytes of the index file is the count of entries.

static inline off_t ifile_ofs(uint64_t id) {
    return 8 + (id << 3);
}

// Read an entry from the index file using the mmap if available.

static inline int ifile_read(struct store *s, uint64_t id, uint64_t *out_ientry) {
    if (id > s->icap)
        return STORE_EINVAL;

    off_t iofs = ifile_ofs(id);

    if (s->imm && s->imm_sz) {
        *out_ientry = *(uint64_t *)(s->imm + iofs);
    } else if (retry(pread(s->ifd, &out_ientry, IENTRY_SZ, iofs)) < IENTRY_SZ) {
        perror("read index entry");
        return STORE_EIO;
    }

    return STORE_OK;
}

// Write an entry to the index file using the mmap if available.

static inline int ifile_write(struct store *s, uint64_t id, uint64_t ofs, uint16_t rev) {
    if (id > s->icap)
        return STORE_EINVAL;

    uint64_t ientry = ientry_make(ofs, rev);
    off_t iofs = ifile_ofs(id);

    if (s->imm && s->imm_sz) {
        *(uint64_t *)(s->imm + iofs) = ientry;
    } else if (retry(pwrite(s->ifd, &ientry, IENTRY_SZ, iofs)) < IENTRY_SZ) {
        perror("updated index entry");
        return STORE_EIO;
    }

    return STORE_OK;
}

int store_put(struct store *s, struct stored *v) {
    if (!s || !v || !v->data || !v->sz) 
        return STORE_EINVAL;

    pthread_mutex_lock(&s->mutex);

    // Get index file entry for id.

    uint64_t ientry = 0;
    if (ifile_read(s, v->id, &ientry) || ientry_rev(ientry) != v->rev) {
        pthread_mutex_unlock(&s->mutex);
        return STORE_EIO;
    }

    // Append record descriptor and record to log file.

    uint64_t desc[2] = { v->id, v->sz };
    struct iovec iov[2] = { 
        { desc, sizeof(desc) },  // descriptor
        { v->data, v->sz }       // record
    };
    if (retry(writev(s->lfd, iov, 2)) < sizeof(desc) + v->sz) {
        perror("append to log");
        pthread_mutex_unlock(&s->mutex);
        return STORE_EIO;
    }

    // Update index file entry.

    int rc = ifile_write(s, v->id, s->lsz, v->rev + 1);
    if (STORE_OK != rc) {
        pthread_mutex_unlock(&s->mutex);
        return rc;
    }

    v->rev++;
    s->lsz += sizeof(desc) + v->sz;

    pthread_mutex_unlock(&s->mutex);
    return STORE_OK;
}

int store_get(struct store *s, struct stored *v) {
    if (!s || !v || v->data) 
        return STORE_EINVAL;

    pthread_mutex_lock(&s->mutex);

    // Get index entry for this id.

    uint64_t ientry;
    int rc = ifile_read(s, v->id, &ientry);
    if (STORE_OK != rc) {
        pthread_mutex_unlock(&s->mutex);
        return rc;
    }
    
    if (ientry == (uint64_t) -1) {
        pthread_mutex_unlock(&s->mutex);
        return STORE_ENOENT;
    }

    // Read the record descriptor from the log.

    uint64_t desc[2] = {0,0};
    if (retry(pread(s->lfd, desc, sizeof(desc), ientry_ofs(ientry))) < sizeof(desc)) {
        perror("read record descriptor");
        pthread_mutex_unlock(&s->mutex);
        return STORE_EIO;
    }

    // Deleted? Never put? Then, id and size will be 0.

    if (0 == desc[0] && 0 == desc[1]) {
        pthread_mutex_unlock(&s->mutex);
        return STORE_ENOENT;
    }

    // Sanity check that the ID in the file is the ID expected.

    if (desc[0] != v->id) {
        pthread_mutex_unlock(&s->mutex);
        return STORE_ETAMPER;
    }

    // Read the log record into user data.

    if (!(v->data = malloc(desc[1]))) {
        perror("malloc for log record data");
        pthread_mutex_unlock(&s->mutex);
        return STORE_ENOMEM;
    }

    if (retry(pread(s->lfd, v->data, desc[1], ientry_ofs(ientry) + sizeof(desc))) < desc[1]) {
        perror("read log record");
        free(v->data);
        pthread_mutex_unlock(&s->mutex);
        return STORE_EIO;
    }

    v->sz  = desc[1];
    v->ofs = ientry_ofs(ientry);
    v->rev = ientry_rev(ientry);

    pthread_mutex_unlock(&s->mutex);
    return STORE_OK;
}

int store_rm(struct store *s, struct stored *v) {
    if (!s || !v) 
        return STORE_EINVAL;

    pthread_mutex_lock(&s->mutex);

    // Clear the index file entry for the ID.
    // Note: we do _not_ free up the ID for reuse.

    int rc = ifile_write(s, v->id, (uint64_t) -1, (uint16_t) -1);
    if (STORE_OK != rc) {
        pthread_mutex_unlock(&s->mutex);
        return rc;
    }

    // Append a "delete record" (size==-1) to the log file.

    uint64_t desc[2] = { v->id, (uint64_t) -1 };
    if (retry(write(s->lfd, desc, sizeof(desc))) < sizeof(desc)) {
        perror("append delete record");
        pthread_mutex_unlock(&s->mutex);
        return STORE_EIO;
    }

    if (v->data) {
        free(v->data);
        v->data = NULL;
    }

    v->sz  = 0;
    v->ofs = (off_t) -1;

    pthread_mutex_unlock(&s->mutex);
    return STORE_OK;
}

int store_sync(struct store *s) {
    if (!s) 
        return STORE_EINVAL;

    pthread_mutex_lock(&s->mutex);

    fsync(s->lfd);

    if (s->imm && s->imm_sz) 
        msync(s->imm, s->imm_sz, MS_SYNC);
    else 
        fsync(s->ifd);

    pthread_mutex_unlock(&s->mutex);
    return STORE_OK;
}

int store_close(struct store *s) {
    if (!s) 
        return STORE_EINVAL;

    pthread_mutex_lock(&s->mutex);

    if (s->imm && s->imm_sz) 
        munmap(s->imm, s->imm_sz);

    close(s->lfd);
    close(s->ifd);

    pthread_mutex_unlock(&s->mutex);
    pthread_mutex_destroy(&s->mutex);

    return STORE_OK;
}

char *store_strerror(int code) {
    switch (code) {
        case STORE_OK:           return "success";
        case STORE_EIO:          return "input/output error";
        case STORE_ENOMEM:       return "out of memory";
        case STORE_EINVAL:       return "bad argument(s)";
        case STORE_ENOENT:       return "no such entity";
        case STORE_ETAMPER:      return "data was tampered with";
    }
    
    return NULL;
}
