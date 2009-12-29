#ifndef LOGSTORE_H
#define LOGSTORE_H

#ifdef __cplusplus
extern "C" 
{
#endif 

struct LogStore;
typedef struct LogStore *LogStore;

enum 
{
    kLogStoreOK,        
    kLogStoreInputOutputError,
    kLogStoreOutOfMemory,
    kLogStoreInvalidParameter,
    kLogStoreNotFound,
    kLogStoreRevisionConflict,
    kLogStoreTampered
};

typedef uint32_t LogStoreID;
typedef uint16_t LogStoreRevision;

/**
 * Opens a logstore.
 *
 * @param path The filesystem path to the log file associated 
 * with the logstore.  A sister file ('path'-index) lives in the
 * same directory.  These files are created as needed; use umask
 * for desired permissions.
 *
 * @param outStore [out] The store to create. The store is dynamically
 * allocated.  Be sure to pass a pointer to a 'LogStore' that is
 * initialized to NULL.
 *
 * @return code (e.g. kLogStoreOK).
 */

int   LogStoreOpen     (LogStore *outStore, const char *path);

/**
 * Closes an open logstore.
 *
 * @param store The store to close. Accepts a pointer to the logstore.
 * The logstore is closed, its memory released, and the pointer is set
 * to NULL.
 *
 * @return code (e.g. kLogStoreOK).
 */

int   LogStoreClose    (LogStore *store);

/**
 * Tries hard to ensure that data that is written to disk (e.g. LogStorePut)
 * has actually been transferred to the disk device and is not sitting in 
 * OS or disk buffers.  Note: *tries*.
 *
 * @param store The store to sync.
 *
 * @return code (e.g. kLogStoreOK).
 */

int   LogStoreSync     (LogStore store);

/** 
 * Entries in the log have unique IDs.  This creates a unique ID
 * so that a new entry may be subsequently stored.
 *
 * @param store The store.
 *
 * @param outID [out] The ID generated.
 *
 * @return code (e.g. kLogStoreOK).
 */

int   LogStoreMakeID   (LogStore store, LogStoreID *outID);

/**
 * Puts or stores a value to the logstore.
 *
 * @param store The store to which the value should be saved.
 *
 * @param id The ID of the value (see LogStoreMakeID).
 *
 * @param data The data to put (must be non-NULL).
 *
 * @param size The size of 'data' in bytes (must be > 0).
 *
 * @param rev The revision of the data.  For new values,
 * use a rev of 0.
 *
 * @return code (e.g. kLogStoreOK).
 */

int   LogStorePut      (LogStore store, LogStoreID id, 
                        void *data, size_t size, 
                        LogStoreRevision rev);

/**
 * Gets or loads a value from the logstore by ID.
 *
 * @param store The store from which the value should be loaded.
 *
 * @param id The ID of the value (see LogStoreMakeID).
 *
 * @param outData [out] A pointer to a NULL-initialized buffer. Logstore
 * will read the value from disk and allocate *outData to point to the
 * value read.  Required. void *data = NULL; LogStoreGet(..., &data, ...)
 *
 * @param outSize [out] The size of the value read in bytes. Optional.
 * If you do not care about the size, pass NULL.
 *
 * @param outRev [out] The current/latest revision/version of the value 
 * in the logstore.  Optional.  If you do not care about the size, pass NULL.
 *
 * @return code (e.g. kLogStoreOK).
 */

int   LogStoreGet      (LogStore store, LogStoreID id, 
                        void **outData, size_t *outSize, 
                        LogStoreRevision *outRev);

/**
 * Removes a value by ID.  Note that IDs should be treated as black
 * box opaque values.  Also, IDs are not recycled.
 *
 * @param store The store from which the value should be removed.
 *
 * @param id The ID of the value to remove.
 *
 * @return code (e.g. kLogStoreOK).
 */

int   LogStoreRemove   (LogStore store, LogStoreID id);

/**
 * Describes in English an error/response code.
 *
 * @param code The response code to the describe (e.g. kLogStoreNotFound).
 */

char *LogStoreDescribe (int code);

#ifdef __cplusplus
}
#endif

#endif
