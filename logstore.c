#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include "logstore.h"
#include "logstore_private.h"

#ifdef O_NOATIME
#define kOtherOpenFlags O_NOATIME
#else
#define kOtherOpenFlags 0
#endif

// This index file growth factor is arbitrary.  The idea is that the index file
// is often going to be memory-mapped for perforemoveance reasons, but
// memory-mapped files cannot grow through a memory-map.  Thus, we treat the
// index file as a sparse file and grow it by seeking+writing beyond the EOF by
// a certain amount and remapping the file.

#define kIndexFileGrowBy (4096/8 * 1000)

#define LogStoreLock   pthread_mutex_lock(&store->mutex)
#define LogStoreUnlock pthread_mutex_unlock(&store->mutex);

typedef uint32_t IndexFileCount;

typedef uint32_t LogFileEntryHeader[2];            // id, size

// Index file entries are 64-bit numbers with high 16 bits for revision, low 48
// bits for log offset.  max revisions: ~65K; max log file size: ~260GiB.  an
// index file is a sparse file wherein the entry for id X is LogStored at byte
// offset X*8.

typedef uint64_t IndexEntry;                       // [rev|offset]

// A LogStore is a log file and an index file (<path>-index)

int LogStoreOpen(LogStore *sp, const char *path) 
{
    if (NULL == sp || NULL != *sp || NULL == path) 
    {
        return kLogStoreInvalidParameter;
    }

    LogStore store = calloc(sizeof(struct LogStore), 1);

    if (!store) 
    {
        return kLogStoreOutOfMemory;
    }

    // Open log file.

    int flags = O_CREAT | O_APPEND | O_RDWR | kOtherOpenFlags;

    if (-1 == (store->logFileNo = open(path, flags, 0777))) 
    {
        free(store);

        return kLogStoreInputOutputError;
    }

    // Get size of log file.

    struct stat logFileStat;

    if (fstat(store->logFileNo, &logFileStat) < 0 || 
        !S_ISREG(logFileStat.st_mode)) 
    {
        close(store->logFileNo);
        free(store);

        return kLogStoreInputOutputError;
    }

    store->logFileSize = logFileStat.st_size;

    // Open index file.

    char *ipath = malloc(strlen(path) + strlen("-index") + 1);

    if (NULL == ipath) 
    {
        return kLogStoreOutOfMemory;
    }

    sprintf(ipath, "%s-index", path);

    store->indexFileNo = open(ipath, O_CREAT | O_RDWR | kOtherOpenFlags, 0777);

    free(ipath);

    if (-1 == store->indexFileNo) 
    {
        close(store->logFileNo);
        free(store);

        return kLogStoreInputOutputError;
    }

    // Deteremoveine the capacity of the index file.

    struct stat indexFileStat;

    if (-1 == fstat(store->indexFileNo, &indexFileStat)) 
    {
        close(store->indexFileNo);
        close(store->logFileNo);
        free(store);

        return kLogStoreInputOutputError;
    }

    store->indexFileCapacity = indexFileStat.st_size / sizeof(IndexEntry);

    // If needed, grow the (sparse) index file to hold a decent number of
    // entries for mmap.

    store->indexFileGrowthCount = 0;

    if (store->indexFileCapacity == 0) 
    {
        char zero = 0;
        off_t newEOF = kIndexFileGrowBy * sizeof(IndexEntry) - sizeof(char);

        int bytesWritten = 0;

        do 
        {
            bytesWritten = pwrite(store->indexFileNo, &zero, 
                                  sizeof(char), newEOF);
        }
        while (bytesWritten == -1 && errno == EINTR);

        if (bytesWritten < sizeof(char)) 
        {
            close(store->indexFileNo);
            close(store->logFileNo);
            free(store);

            return kLogStoreInputOutputError;
        }

        store->indexFileCapacity = kIndexFileGrowBy;
        store->indexFileGrowthCount++;
    }

    // Get number of entries in the LogStore from the beginning 
    // of the index file.

    int bytesRead = 0;

    do 
    {
        bytesRead = read(store->indexFileNo, &store->indexFileCount, 
                         sizeof(store->indexFileCount));
    }
    while (bytesRead == -1 && errno == EINTR);

    if (bytesRead < sizeof(store->indexFileCount)) 
    {
        close(store->logFileNo);
        close(store->indexFileNo);
        free(store);

        return kLogStoreInputOutputError;
    }

    // Try to mmap the index file; falls back to regular file i/o on failure.

    store->indexFileMappingSize = store->indexFileCapacity * sizeof(IndexEntry);

    store->indexFileMapping = mmap(0, store->indexFileMappingSize, 
                                   PROT_READ | PROT_WRITE, 
                                   MAP_SHARED, store->indexFileNo, 0);

    if (MAP_FAILED == store->indexFileMapping) 
    {
        store->indexFileMapping = NULL;
        store->indexFileMappingSize = 0;
    }

    // Create a mutex.

    pthread_mutex_init(&store->mutex, NULL);

    *sp = store;

    return kLogStoreOK;
}

int LogStoreMakeID(LogStore store, LogStoreID *outID) 
{
    if (NULL == store || NULL == outID) 
    {
        return kLogStoreInvalidParameter;
    }

    LogStoreLock;

    *outID = store->indexFileCount++;

    // Save the number of used index entries in the index file (at offset 0).

    if (store->indexFileMapping && store->indexFileMappingSize) 
    {
        *(IndexFileCount *)store->indexFileMapping = store->indexFileCount;
    } 
    else 
    {
        int bytesWritten = 0;

        do 
        {
            bytesWritten = pwrite(store->indexFileNo, &store->indexFileCount, 
                                  sizeof(store->indexFileCount), 0);
        }
        while (bytesWritten == -1 && errno == EINTR);

        if (bytesWritten < sizeof(store->indexFileCount)) 
        {
            LogStoreUnlock;

            return kLogStoreInputOutputError;
        }
    }

    // If the index file is too big and we're using mmap to access its content,
    // unmap, grow the file, and remap.

    if (store->indexFileCount == store->indexFileCapacity && 
        store->indexFileMapping && store->indexFileMappingSize) 
    {
        char zero = 0;
        off_t newSize;

        munmap(store->indexFileMapping, store->indexFileMappingSize);

        store->indexFileCapacity += kIndexFileGrowBy;
        newSize = store->indexFileCapacity * sizeof(IndexEntry);

        int bytesWritten = 0;

        do 
        {
            bytesWritten = pwrite(store->indexFileNo, &zero, sizeof(char), 
                                  newSize - sizeof(char));
        }
        while (bytesWritten == -1 && errno == EINTR);

        if (bytesWritten < sizeof(char)) 
        {
            LogStoreUnlock;

            return kLogStoreInputOutputError;
        }

        store->indexFileGrowthCount++;

        store->indexFileMapping = mmap(0, newSize, PROT_READ | PROT_WRITE, 
                                       MAP_SHARED, store->indexFileNo, 0);

        if (MAP_FAILED == store->indexFileMapping) 
        {
            store->indexFileMapping = NULL;
            store->indexFileMappingSize = 0;
        } 
        else 
        {
            store->indexFileMappingSize = newSize;
        }
    }

    LogStoreUnlock;

    return kLogStoreOK;
}

// Get the log-file offset given an index file entry.

static inline off_t indexEntryGetOffset(IndexEntry e) 
{
    return e & 0x0000ffffffffffff;
}

// Get the revision of a given index file entry.

static inline LogStoreRevision indexEntryGetRevision(IndexEntry e) 
{
    return (e & 0xffff000000000000) >> 48;
}

// Make an index file entry (offset and revision).

static inline IndexEntry indexEntryMake(off_t ofs, LogStoreRevision rev) 
{
    IndexEntry e = rev;

    e <<= 48;
    e |= (ofs & 0x0000ffffffffffff);

    return e;
}

// The index file starts with a count then continues with N entries.

static inline off_t indexFileOffsetOf(LogStoreID id) 
{
    return sizeof(IndexFileCount) + (id * sizeof(IndexEntry));
}

// Read an entry from the index file using the mmap if available.

static inline int indexFileRead(LogStore    store, 
                                LogStoreID  id, 
                                IndexEntry *outIndexEntry) 
{
    if (id > store->indexFileCapacity)
    {
        return kLogStoreInvalidParameter;
    }

    off_t offset = indexFileOffsetOf(id);

    if (store->indexFileMapping && store->indexFileMappingSize) 
    {
        *outIndexEntry = *(IndexEntry *)((char *)store->indexFileMapping + 
                                         offset);
    } 
    else 
    {
        int bytesRead = 0;

        do 
        {
            bytesRead = pread(store->indexFileNo, outIndexEntry, 
                           sizeof(IndexEntry), offset);
        }
        while (bytesRead == -1 && errno == EINTR);

        if (bytesRead < sizeof(IndexEntry)) 
        {
            return kLogStoreInputOutputError;
        }
    }

    return kLogStoreOK;
}

// Write an entry to the index file using the mmap if available.

static inline int indexFileWrite(LogStore         store, 
                                 LogStoreID       id, 
                                 off_t            newEntryOffset, 
                                 LogStoreRevision newEntryRevision) 
{
    if (id >= store->indexFileCapacity)
    {
        return kLogStoreInvalidParameter;
    }

    IndexEntry entry = indexEntryMake(newEntryOffset, newEntryRevision);

    off_t offset = indexFileOffsetOf(id);

    if (store->indexFileMapping && store->indexFileMappingSize) 
    {
        *(IndexEntry *)((char *)store->indexFileMapping + offset) = entry;
    } 
    else 
    {
        int bytesWritten = 0;

        do 
        {
            bytesWritten = pwrite(store->indexFileNo, &entry, 
                                  sizeof(IndexEntry), offset);
        }
        while (bytesWritten == -1 && errno == EINTR);

        if (bytesWritten < sizeof(IndexEntry)) 
        {
            return kLogStoreInputOutputError;
        }
    }

    return kLogStoreOK;
}

int LogStorePut(LogStore          store, 
                LogStoreID        id, 
                void             *data, 
                size_t            size, 
                LogStoreRevision  rev) 
{
    if (NULL == store || NULL == data || 0 == size) 
    {
        return kLogStoreInvalidParameter;
    }

    LogStoreLock;

    // Get index file entry for id.

    IndexEntry e = 0;

    if (indexFileRead(store, id, &e)) 
    {
        LogStoreUnlock;

        return kLogStoreInputOutputError;
    }

    // Check for a version conflict.

    if (indexEntryGetRevision(e) != rev) 
    {
        LogStoreUnlock;

        return kLogStoreRevisionConflict;
    }

    // Append record descriptor and record to log file.

    LogFileEntryHeader header = { id, size };

    struct iovec iov[2] = 
    {
        { header, sizeof(header) },
        { data, size }
    };

    int bytesWritten = 0;

    do 
    {
        bytesWritten = writev(store->logFileNo, iov, 2);
    }
    while (bytesWritten == -1 && errno == EINTR);

    if (bytesWritten < sizeof(header) + size) 
    {
        LogStoreUnlock;

        return kLogStoreInputOutputError;
    }

    // Update index file entry.  The new offset is the size of the log prior to
    // the record descriptor being written.  The new revision is 1 greater than
    // the current revision.

    int result = indexFileWrite(store, id, store->logFileSize, rev + 1);

    if (kLogStoreOK != result) 
    {
        LogStoreUnlock;

        return result;
    }

    store->logFileSize += sizeof(header) + size;

    LogStoreUnlock;

    return kLogStoreOK;
}

int LogStoreGet(LogStore          store, 
                LogStoreID        id, 
                void            **outData, 
                size_t           *outSize, 
                LogStoreRevision *outRev) 
{
    if (NULL == store || NULL == outData || NULL != *outData) 
    {
        return kLogStoreInvalidParameter;
    }

    LogStoreLock;

    // Get index entry for this id.

    IndexEntry entry;

    int result = indexFileRead(store, id, &entry);

    if (kLogStoreOK != result) 
    {
        LogStoreUnlock;

        return result;
    }

    // Deleted?

    if ((IndexEntry) -1 == entry)
    {
        LogStoreUnlock;

        return kLogStoreNotFound;
    }

    off_t            entryOffset   = indexEntryGetOffset(entry);
    LogStoreRevision entryRevision = indexEntryGetRevision(entry);

    // Read the record descriptor from the log.

    LogFileEntryHeader header = { 0, 0 };

    int bytesRead = 0;

    do 
    {
        bytesRead = pread(store->logFileNo, header, sizeof(header), entryOffset);
    }
    while (bytesRead == -1 && errno == EINTR);

    if (bytesRead < sizeof(header)) 
    {
        LogStoreUnlock;

        return kLogStoreInputOutputError;
    }

    // Sanity check that the ID in the file is the ID expected.

    if (header[0] != id || header[1] == 0) 
    {
        LogStoreUnlock;

        return kLogStoreTampered;
    }

    // Read the log record into user data.

    *outData = malloc(header[1]);

    if (NULL == *outData) 
    {
        LogStoreUnlock;

        return kLogStoreOutOfMemory;
    }

    off_t entryDataOffset = entryOffset + sizeof(header);

    bytesRead = 0;

    do 
    {
        bytesRead = pread(store->logFileNo, *outData, 
                          header[1], entryDataOffset);
    }
    while (bytesRead == -1 && errno == EINTR);

    if (bytesRead < header[1]) 
    {
        free(*outData);

        LogStoreUnlock;

        return kLogStoreInputOutputError;
    }

    if (outSize) 
    {
        *outSize = header[1];
    }

    if (outRev)  
    {
        *outRev = entryRevision;
    }

    LogStoreUnlock;

    return kLogStoreOK;
}

int LogStoreRemove(LogStore store, LogStoreID id) 
{
    if (!store) 
    {
        return kLogStoreInvalidParameter;
    }

    LogStoreLock;

    // Clear the index file entry for the ID.
    // Note: we do _not_ free up the ID for reuse.

    int result = indexFileWrite(store, id, (off_t) -1, (LogStoreRevision) -1);

    if (kLogStoreOK != result) 
    {
        LogStoreUnlock;

        return result;
    }

    // Append a "delete record" to the log file.

    LogFileEntryHeader header = { id, 0 };

    int bytesWritten = 0;

    do 
    {
        bytesWritten = write(store->logFileNo, header, sizeof(header));
    }
    while (bytesWritten == -1 && errno == EINTR);

    if (bytesWritten < sizeof(header)) 
    {
        LogStoreUnlock;

        return kLogStoreInputOutputError;
    }

    LogStoreUnlock;

    return kLogStoreOK;
}

int LogStoreSync(LogStore store) 
{
    if (!store) 
    {
        return kLogStoreInvalidParameter;
    }

    LogStoreLock;

    fsync(store->logFileNo);

    if (store->indexFileMapping && store->indexFileMappingSize) 
    {
        msync(store->indexFileMapping, store->indexFileMappingSize, MS_SYNC);
    }
    else 
    {
        fsync(store->indexFileNo);
    }

    LogStoreUnlock;

    return kLogStoreOK;
}

int LogStoreClose(LogStore *sp) 
{
    if (NULL == sp || NULL == *sp) 
    {
        return kLogStoreInvalidParameter;
    }

    LogStore store = *sp;

    LogStoreLock;

    if (store->indexFileMapping && store->indexFileMappingSize) 
    {
        munmap(store->indexFileMapping, store->indexFileMappingSize);
    }

    close(store->logFileNo);
    close(store->indexFileNo);

    LogStoreUnlock;

    pthread_mutex_destroy(&store->mutex);

    return kLogStoreOK;
}

char *LogStoreDescribe(int code) 
{
    switch (code) 
    {
        case kLogStoreOK:               return "success";
        case kLogStoreInputOutputError: return "input/output error";
        case kLogStoreOutOfMemory:      return "out of memory";
        case kLogStoreInvalidParameter: return "bad argument(s)";
        case kLogStoreNotFound:         return "no such entity";
        case kLogStoreTampered:         return "data was tampered with";
        case kLogStoreRevisionConflict: return "revision conflict";
    }

    return NULL;
}
