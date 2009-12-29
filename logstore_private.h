#ifndef LOGSTORE_PRIVATE_H
#define LOGSTORE_PRIVATE_H

#ifdef __cplusplus
extern "C" 
{
#endif

struct LogStore 
{
    int             logFileNo;
    off_t           logFileSize;

    int             indexFileNo;
    int             indexFileCapacity;
    int             indexFileCount;
    int             indexFileGrowthCount;
    void           *indexFileMapping;
    size_t          indexFileMappingSize;

    pthread_mutex_t mutex;
};

#ifdef __cplusplus
}
#endif

#endif
