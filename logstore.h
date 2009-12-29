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

int   LogStoreOpen     (LogStore *outStore, const char *path);
int   LogStoreClose    (LogStore *store);
int   LogStoreSync     (LogStore store);
int   LogStoreMakeID   (LogStore store, LogStoreID *outID);
int   LogStorePut      (LogStore store, LogStoreID id, 
                        void *data, size_t size, 
                        LogStoreRevision rev);
int   LogStoreGet      (LogStore store, LogStoreID id, 
                        void **outData, size_t *outSize, 
                        LogStoreRevision *outRev);
int   LogStoreRemove   (LogStore store, LogStoreID id);
char *LogStoreDescribe (int code);

#ifdef __cplusplus
}
#endif

#endif
