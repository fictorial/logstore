#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h> 
#include <unistd.h>
#include <assert.h>
#include <stdint.h>

#include "logstore_private.h"
#include "logstore.h"

#define kEntryCount 1000

// The log size is initially 0 and there are no entries in the corresponding
// index file.  index file should be grown to non-zero size upon creating a new
// log.  we should be able to map a small index file into memory.

void testOpenNewLog() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));
    assert(s->indexFileNo > 2); // [0,2] stdin/out/err
    assert(s->logFileNo > 2);
    assert(s->logFileSize == 0);
    assert(s->indexFileCount == 0);
    assert(s->indexFileCapacity > 0);
    assert(s->indexFileMapping != NULL);
    assert(s->indexFileMappingSize > 0);
    assert(kLogStoreOK == LogStoreClose(&s));
}

void testOpenExistingButEmptyLog() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));
    assert(s->indexFileNo > 2); // [0,2] stdin/out/err
    assert(s->logFileNo > 2);
    assert(s->logFileSize == 0);
    assert(s->indexFileCount == 0);
    assert(s->indexFileCapacity > 0);
    assert(s->indexFileMapping != NULL);
    assert(s->indexFileMappingSize > 0);
    assert(kLogStoreOK == LogStoreClose(&s));
}

void testIdGeneration() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));
    for (int i=0; i<kEntryCount; ++i) 
    {
        LogStoreID id;
        assert(kLogStoreOK == LogStoreMakeID(s, &id));
        assert(id == i);
    }
    assert(kLogStoreOK == LogStoreClose(&s));
}

void testPut() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));
    for (int i=0; i<kEntryCount; ++i) 
    {
        assert(kLogStoreOK == LogStorePut(s, i, &i, sizeof(i), 0));
    }
    struct stat lst;
    assert(fstat(s->logFileNo, &lst) != -1);
    assert(lst.st_size == kEntryCount * (sizeof(uint32_t)*2 + sizeof(int)));
    assert(kLogStoreOK == LogStoreClose(&s));
}

void testOpenExistingNonEmptyLog() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));
    assert(s->indexFileNo > 2); // [0,2] stdin/out/err
    assert(s->logFileNo > 2);
    assert(s->logFileSize == kEntryCount * (sizeof(uint32_t)*2 + sizeof(int)));
    assert(s->indexFileCount == kEntryCount);
    assert(s->indexFileCapacity >= kEntryCount);
    assert(s->indexFileMapping != NULL);
    assert(s->indexFileMappingSize > 0);
    assert(kLogStoreOK == LogStoreClose(&s));
}

void testGet() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));
    for (int i=0; i<kEntryCount; ++i) 
    {
        void *data = NULL;
        size_t size = 0;
        assert(kLogStoreOK == LogStoreGet(s, i, &data, &size, NULL));
        assert(size == sizeof(int));
        assert(*(int *)data == i);
        free(data);
    }
    assert(kLogStoreOK == LogStoreClose(&s));
}

// Get 2 copies of entry with ID 0.  Put one of the copies back so its revision
// gets updated.  This should increment 'a.rev' given a successful put.  Then
// put 'b' which should fail due to a conflict as 'b' is out of date w.r.t. the
// 'latest' revision of entry with ID 0.

void testConfictDetection() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    void *dataA = NULL;
    void *dataB = NULL;
    size_t sizeA, sizeB;

    LogStoreRevision revisionA, revisionB;
    assert(kLogStoreOK == LogStoreGet(s, 0, &dataA, &sizeA, &revisionA));
    assert(kLogStoreOK == LogStoreGet(s, 0, &dataB, &sizeB, &revisionB));
    assert(kLogStoreOK == LogStorePut(s, 0, dataA, sizeA, revisionA));

    // at this point the revision of entry with id 0 is 2

    assert(kLogStoreRevisionConflict == LogStorePut(s, 0, dataB, sizeB, 
                                                    revisionB));
    assert(kLogStoreOK == LogStoreClose(&s));

    free(dataA);
    free(dataB);
}

void testRemove() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    void *data = NULL;
    size_t size;
    assert(kLogStoreOK == LogStoreGet(s, 0, &data, &size, NULL));
    free(data);
    data = NULL;

    assert(kLogStoreOK == LogStoreRemove(s, 0));

    // removed!
    assert(kLogStoreNotFound == LogStoreGet(s, 0, &data, NULL, NULL));

    // way beyond any existing entry's ID:
    assert(kLogStoreInvalidParameter == LogStoreRemove(s, (LogStoreID) -1));    
    assert(kLogStoreOK == LogStoreClose(&s));
}

int main(int argc, char **argv) 
{
    unlink("log");
    unlink("log-index");

    testOpenNewLog();
    testOpenExistingButEmptyLog();
    testIdGeneration();
    testPut();
    testOpenExistingNonEmptyLog();
    testGet();
    testConfictDetection();
    testRemove();

    return 0;
}
