#pragma once

#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <pthread.h>

struct store {
    int lfd, ifd;
    off_t lsz;
    void *imm;
    size_t imm_sz;
    uint64_t icount, icap;
    uint32_t igrowths;
    pthread_mutex_t mutex;
};

struct stored {
    uint64_t id;
    off_t ofs;
    size_t sz;
    uint16_t rev;
    void *data;
};

enum {
    STORE_OK,           // success
    STORE_EIO,          // input/output error
    STORE_ENOMEM,       // out of memory
    STORE_EINVAL,       // bad argument(s)
    STORE_ENOENT,       // no such entity
    STORE_ETAMPER,      // data was tampered with
};

int store_open(struct store *s, const char *path);
int store_sync(struct store *s);
int store_close(struct store *s);
int store_genid(struct store *s, struct stored *v);
int store_put(struct store *s, struct stored *v);
int store_get(struct store *s, struct stored *v);
int store_rm(struct store *s, struct stored *v);
char *store_strerror(int code);

#define STORED_PUT_INITIALIZER(data,sz) { 0, 0, sz, 0, data }
#define STORED_GET_INITIALIZER(id) { id, 0, 0, 0, NULL }

