#ifndef LOGSTORE_PRIVATE_H
#define LOGSTORE_PRIVATE_H

#ifdef __cplusplus
extern "C" 
{
#endif

struct LogStore 
{
    int             logFileNo;
    int             indexFileNo;
    int             indexFileCapacity;
    int             indexFileCount;
    int             indexFileGrowthCount;
    off_t           logFileSize;
    void           *indexFileMapping;
    size_t          indexFileMappingSize;
    pthread_mutex_t mutex;
};

#ifdef __cplusplus
}
#endif

#endif
