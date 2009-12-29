#ifndef STORE_H
#define STORE_H

#ifdef __cplusplus
extern "C" {
#endif 

struct store;
typedef struct store *store;

typedef enum {
    STORE_OK,        // success
    STORE_EIO,       // input/output error
    STORE_ENOMEM,    // out of memory
    STORE_EINVAL,    // bad argument(s)
    STORE_ENOENT,    // no such entity
    STORE_ECONFLICT, // version conflict
    STORE_ETAMPER    // data was tampered with
} store_rc;

typedef uint64_t store_id;
typedef uint16_t store_revision;

store_rc store_open  (store *, const char *path);
store_rc store_close (store *);
store_rc store_sync  (store);
store_rc store_genid (store, store_id *);
store_rc store_put   (store, store_id, void *, size_t, store_revision);
store_rc store_get   (store, store_id, void **, size_t *, store_revision *);
store_rc store_rm    (store, store_id);
char *store_strerror (store_rc code);

#ifdef __cplusplus
}
#endif

#endif
