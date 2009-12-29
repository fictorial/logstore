#ifndef LOGSTORE_H
#define LOGSTORE_H

#ifdef __cplusplus
extern "C" {
#endif 

struct logstore;
typedef struct logstore *logstore;

typedef enum {
    LOGSTORE_OK,        // success
    LOGSTORE_EIO,       // input/output error
    LOGSTORE_ENOMEM,    // out of memory
    LOGSTORE_EINVAL,    // bad argument(s)
    LOGSTORE_ENOENT,    // no such entity
    LOGSTORE_ECONFLICT, // version conflict
    LOGSTORE_ETAMPER    // data was tampered with
} logstore_rc;

typedef uint64_t logstore_id;
typedef uint16_t logstore_revision;

logstore_rc logstore_open  (logstore *, const char *path);
logstore_rc logstore_close (logstore *);
logstore_rc logstore_sync  (logstore);
logstore_rc logstore_genid (logstore, logstore_id *);
logstore_rc logstore_put   (logstore, logstore_id, void *, size_t, logstore_revision);
logstore_rc logstore_get   (logstore, logstore_id, void **, size_t *, logstore_revision *);
logstore_rc logstore_rm    (logstore, logstore_id);
char *logstore_strerror (logstore_rc code);

#ifdef __cplusplus
}
#endif

#endif
