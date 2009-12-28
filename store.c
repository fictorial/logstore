#include "store.h"
#include <sys/stat.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <sys/mman.h>

#ifdef O_NOATIME
#define OTHER_OPEN_FLAGS O_NOATIME
#else
#define OTHER_OPEN_FLAGS 0
#endif

#define retry(expr) ({ \
    int __rc; \
    do __rc = (expr); \
    while (__rc == -1 && errno == EINTR); __rc; })

#define IFILE_GROW_BY   10000
#define IENTRY_SZ       8

// A store is a log file and an index file (path.i)

int store_open(struct store *s, const char *path) {
    if (!s || !path) return 1;

    // Open log file.

    if (-1 == (s->lfd = open(path, O_CREAT|O_APPEND|O_RDWR|OTHER_OPEN_FLAGS, 0777))) {
        perror("open log file");
        return 1;
    }

    // Get size of log file.

    struct stat st;
    if (fstat(s->lfd, &st) < 0 || !S_ISREG(st.st_mode)) {
        perror("open log file");
        close(s->lfd);
        return 1;
    }
    s->lsz = st.st_size;

    // Open index file.

    char *ipath = malloc(strlen(path) + strlen("__index") + 1);
    sprintf(ipath, "%s__index", path);
    s->ifd = open(ipath, O_CREAT|O_RDWR|OTHER_OPEN_FLAGS, 0777);
    free(ipath);
    if (-1 == s->ifd) {
        perror("open index file");
        close(s->lfd);
        return 1;
    }

    // Determine the capacity of the index file.

    struct stat ist;
    if (fstat(s->ifd, &ist) < 0) {
        perror("index file fstat");
        close(s->ifd);
        close(s->lfd);
        return 1;
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
            return 1;
        }
        s->icap = IFILE_GROW_BY;
        s->igrowths++;
    }

    // Get number of entries in the store from the beginning of the index file.

    if (retry(read(s->ifd, &s->icount, 8)) < 8) {
        perror("read entry count");
        close(s->lfd);
        close(s->ifd);
        return 1;
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

    return 0;
}

int store_genid(struct store *s, struct stored *v) {
    if (!s || !v) return 1;

    v->id = s->icount++;

    // Save the number of used index entries in the index file (at offset 0).

    if (s->imm && s->imm_sz) {
        *(uint64_t *)s->imm = s->icount;
    } else if (retry(pwrite(s->ifd, &s->icount, 8, 0)) < 8) {
        perror("write next id");
        return 1;
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
            return 1;
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

    return 0;
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
        return 1;

    off_t iofs = ifile_ofs(id);

    if (s->imm && s->imm_sz) {
        *out_ientry = *(uint64_t *)(s->imm + iofs);
    } else if (retry(pread(s->ifd, &out_ientry, IENTRY_SZ, iofs)) < IENTRY_SZ) {
        perror("read index entry");
        return 1;
    }

    return 0;
}

// Write an entry to the index file using the mmap if available.

static inline int ifile_write(struct store *s, uint64_t id, uint64_t ofs, uint16_t rev) {
    if (id > s->icap)
        return 1;

    uint64_t ientry = ientry_make(ofs, rev);
    off_t iofs = ifile_ofs(id);

    if (s->imm && s->imm_sz) {
        *(uint64_t *)(s->imm + iofs) = ientry;
    } else if (retry(pwrite(s->ifd, &ientry, IENTRY_SZ, iofs)) < IENTRY_SZ) {
        perror("updated index entry");
        return 1;
    }

    return 0;
}

int store_put(struct store *s, struct stored *v) {
    if (!s || !v || !v->data || !v->sz) return 1;

    // Get index file entry for id.

    uint64_t ientry = 0;
    if (ifile_read(s, v->id, &ientry) || ientry_rev(ientry) != v->rev) 
        return 1;

    // Append record descriptor and record to log file.

    uint64_t desc[2] = { v->id, v->sz };
    struct iovec iov[2] = { 
        { desc, sizeof(desc) },  // descriptor
        { v->data, v->sz }       // record
    };
    if (retry(writev(s->lfd, iov, 2)) < sizeof(desc) + v->sz) {
        perror("append to log");
        return 1;
    }

    // Update index file entry.

    if (ifile_write(s, v->id, s->lsz, v->rev + 1)) 
        return 1;

    v->rev++;
    s->lsz += sizeof(desc) + v->sz;
    return 0;
}

int store_get(struct store *s, struct stored *v) {
    if (!s || !v || v->data) return 1;

    // Get index entry for this id.

    uint64_t ientry;
    if (ifile_read(s, v->id, &ientry)) 
        return 1;

    // Read the record descriptor from the log.

    uint64_t desc[2] = {0,0};
    if (retry(pread(s->lfd, desc, sizeof(desc), ientry_ofs(ientry))) < sizeof(desc)) {
        perror("read record descriptor");
        return 1;
    }

    // Deleted? Never put? Then, id and size will be 0.

    if (0 == desc[0] && 0 == desc[1])
        return 1;

    // Sanity check that the ID in the file is the ID expected.

    if (desc[0] != v->id) 
        return 1;

    // Read the log record into user data.

    if (!(v->data = malloc(desc[1]))) {
        perror("malloc for log record data");
        return 1;
    }

    if (retry(pread(s->lfd, v->data, desc[1], ientry_ofs(ientry) + sizeof(desc))) < desc[1]) {
        perror("read log record");
        free(v->data);
        return 1;
    }

    v->sz  = desc[1];
    v->ofs = ientry_ofs(ientry);
    v->rev = ientry_rev(ientry);

    return 0;
}

int store_rm(struct store *s, struct stored *v) {
    if (!s || !v) return 1;

    // Clear the index file entry for the ID.
    // Note: we do _not_ free up the ID for reuse.

    uint64_t ientry = 0;
    if (ifile_write(s, v->id, 0, 0)) 
        return 1;

    // Append a "delete record" (size==0) to the log file.

    uint64_t desc[2] = { v->id, 0 };
    if (retry(write(s->lfd, desc, sizeof(desc))) < sizeof(desc)) {
        perror("append delete record");
        return 1;
    }

    return 0;
}

int store_sync(struct store *s) {
    if (!s) return 1;

    fsync(s->lfd);

    if (s->imm && s->imm_sz) 
        msync(s->imm, s->imm_sz, MS_SYNC);
    else 
        fsync(s->ifd);

    return 0;
}

int store_close(struct store *s) {
    if (!s) return 1;
    store_sync(s);
    if (s->imm && s->imm_sz) 
        munmap(s->imm, s->imm_sz);
    close(s->lfd);
    close(s->ifd);
    return 0;
}
