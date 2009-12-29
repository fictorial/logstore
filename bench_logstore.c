#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h> 
#include <time.h>
#include <unistd.h>

#include "logstore_private.h"
#include "logstore.h"

#define TIME_DELTA_MICRO(start, end) \
    (((double)end.tv_sec   * 1000000.0 + (double)end.tv_usec) \
   - ((double)start.tv_sec * 1000000.0 + (double)start.tv_usec)) 

#define TIME_DELTA_SECONDS(start, end) (TIME_DELTA_MICRO(start, end) / 1e6)

#define kPutCount 200000

uint64_t firstPutIntID = 0;

void benchmarkPutsNoSyncIntValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end; 
    gettimeofday(&start, NULL);

    for (int i=0; i<kPutCount; ++i) 
    {
        LogStoreID id;
        assert(kLogStoreOK == LogStoreMakeID(s, &id));
        assert(kLogStoreOK == LogStorePut(s, id, &i, sizeof(int), 0));

        if (i == 0) 
        {
            firstPutIntID = id;
        }
    }

    gettimeofday(&end, NULL);
    double putsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)putsPerSec);

    printf("%s: %d index file growths performed\n", 
           __FUNCTION__, s->indexFileGrowthCount);

    assert(kLogStoreOK == LogStoreClose(&s));
}

void benchmarkPutsSyncEveryPutIntValue() 
{
    printf("%s: this might take a while...\n", __FUNCTION__);

    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end; 
    gettimeofday(&start, NULL);

    for (int i=0; i<kPutCount; ++i) 
    {
        LogStoreID id;
        assert(kLogStoreOK == LogStoreMakeID(s, &id));
        assert(kLogStoreOK == LogStorePut(s, id, &i, sizeof(int), 0));
        assert(kLogStoreOK == LogStoreSync(s));
    }

    gettimeofday(&end, NULL);
    double putsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);

    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)putsPerSec);

    printf("%s: %d index file growths performed\n", 
           __FUNCTION__, s->indexFileGrowthCount);

    assert(kLogStoreOK == LogStoreClose(&s));
}

void benchmarkPutsSyncOncePerSecondIntValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end, second_start, now;
    gettimeofday(&start, NULL);
    gettimeofday(&second_start, NULL);

    int syncs = 0;

    for (int i=0; i<kPutCount; ++i) 
    {
        LogStoreID id;
        assert(kLogStoreOK == LogStoreMakeID(s, &id));
        assert(kLogStoreOK == LogStorePut(s, id, &i, sizeof(int), 0));

        gettimeofday(&now, NULL);

        if (TIME_DELTA_SECONDS(second_start, now) >= 1) 
        {
            assert(kLogStoreOK == LogStoreSync(s));
            gettimeofday(&second_start, NULL);
            syncs++;
        }
    }

    gettimeofday(&end, NULL);
    double putsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);

    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)putsPerSec);
    printf("%s: %d syncs performed\n", __FUNCTION__, syncs);

    printf("%s: %d index file growths performed\n", 
           __FUNCTION__, s->indexFileGrowthCount);

    assert(kLogStoreOK == LogStoreClose(&s));
}

LogStoreID firstPut1KiBID = 0;

void benchmarkPutsNoSync1KiBValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end; 
    gettimeofday(&start, NULL);

    char *data = malloc(1024);

    for (int i=0; i<kPutCount; ++i) 
    {
        LogStoreID id;
        assert(kLogStoreOK == LogStoreMakeID(s, &id));
        assert(kLogStoreOK == LogStorePut(s, id, data, 1024, 0));

        if (i == 0) 
        {
            firstPut1KiBID = id;
        }
    }

    gettimeofday(&end, NULL);
    free(data);

    double putsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)putsPerSec);

    printf("%s: %d index file growths performed\n", 
           __FUNCTION__, s->indexFileGrowthCount);

    assert(kLogStoreOK == LogStoreClose(&s));
}

void benchmarkPutsSyncOncePerSecond1KiBValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    char *data = malloc(1024);

    struct timeval start, end, second_start, now;
    gettimeofday(&start, NULL);
    gettimeofday(&second_start, NULL);

    int syncs = 0;

    for (int i=0; i<kPutCount; ++i) 
    {
        LogStoreID id;
        assert(kLogStoreOK == LogStoreMakeID(s, &id));
        assert(kLogStoreOK == LogStorePut(s, id, data, 1024, 0));

        gettimeofday(&now, NULL);

        if (TIME_DELTA_SECONDS(second_start, now) >= 1) 
        {
            assert(kLogStoreOK == LogStoreSync(s));
            gettimeofday(&second_start, NULL);
            syncs++;
        }
    }

    gettimeofday(&end, NULL);
    free(data);

    double putsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)putsPerSec);
    printf("%s: %d syncs performed\n", __FUNCTION__, syncs);

    printf("%s: %d index file growths performed\n", 
           __FUNCTION__, s->indexFileGrowthCount);

    assert(kLogStoreOK == LogStoreClose(&s));
}

void benchmarkSequentialGetsIntValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end; 
    gettimeofday(&start, NULL);

    for (int i=0; i<kPutCount; ++i) 
    {
        void *data = NULL;
        size_t size = 0;
        assert(kLogStoreOK == LogStoreGet(s, firstPutIntID + i, &data, 
                                          &size, NULL));
        assert(NULL != data);
        assert(size == sizeof(int));
        assert(*(int *)data == i);
        free(data);
    }

    gettimeofday(&end, NULL);
    double getsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);

    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)getsPerSec);
    assert(kLogStoreOK == LogStoreClose(&s));
}

void benchmarkRandomGetsIntValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end; 
    gettimeofday(&start, NULL);

    srand(time(NULL));

    for (int i=0; i<kPutCount; ++i) 
    {
        LogStoreID randomID = (rand() / (double)RAND_MAX) * kPutCount;

        void *data = NULL;
        size_t size = 0;
        assert(kLogStoreOK == LogStoreGet(s, firstPutIntID + randomID, 
                                          &data, &size, NULL));
        assert(NULL != data);
        assert(size == sizeof(int));
        assert(*(int *)data == randomID);
        free(data);
    }

    gettimeofday(&end, NULL);
    double getsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)getsPerSec);

    assert(kLogStoreOK == LogStoreClose(&s));
}

void benchmarkSequentialGets1KiBValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end; 
    gettimeofday(&start, NULL);

    for (int i=0; i<kPutCount; ++i) 
    {
        void *data = NULL;
        size_t size = 0;
        assert(kLogStoreOK == LogStoreGet(s, firstPut1KiBID + i, 
                                          &data, &size, NULL));
        assert(NULL != data);
        assert(size == 1024);
        free(data);
    }

    gettimeofday(&end, NULL);
    double getsPerSec = kPutCount / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)getsPerSec);

    assert(kLogStoreOK == LogStoreClose(&s));
}

void benchmarkRandomGets1KiBValue() 
{
    LogStore s = NULL;
    assert(kLogStoreOK == LogStoreOpen(&s, "log"));

    struct timeval start, end; 
    gettimeofday(&start, NULL);
    srand(time(NULL));

    // Limit this so it finishes in a decent amount of time.  Reads/gets are
    // much slower than writes/puts on average since they require a disk seek
    // whereas an append basically does not.

    for (int i=0; i<1000; ++i) 
    {
        LogStoreID randomID = (rand() / (double)RAND_MAX) * 1000;
        void *data = NULL;
        size_t size = 0;
        assert(kLogStoreOK == LogStoreGet(s, firstPut1KiBID + randomID, 
                                          &data, &size, NULL));
        assert(NULL != data);
        assert(size == 1024);
        free(data);
    }

    gettimeofday(&end, NULL);
    double getsPerSec = 1000 / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)getsPerSec);

    assert(kLogStoreOK == LogStoreClose(&s));
}

int main(int argc, char **argv) 
{
    unlink("log");
    unlink("log-index");

    benchmarkPutsNoSyncIntValue();
    // VERY slow on mac os x at least.
    //benchmarkPutsSyncEveryPutIntValue();
    benchmarkPutsSyncOncePerSecondIntValue();
    benchmarkPutsNoSync1KiBValue();
    benchmarkPutsSyncOncePerSecond1KiBValue();
    benchmarkSequentialGetsIntValue();
    benchmarkRandomGetsIntValue();
    benchmarkSequentialGets1KiBValue();
    benchmarkRandomGets1KiBValue();
    
    return 0;
}
