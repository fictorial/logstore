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

#define PUT_COUNT 200000

uint64_t first_put_int_id = 0;

void benchmark_puts_no_sync_int_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        logstore_id id;
        assert(LOGSTORE_OK == logstore_genid(s, &id));
        assert(LOGSTORE_OK == logstore_put(s, id, &i, sizeof(int), 0));
        if (i == 0) first_put_int_id = id;
    }
    gettimeofday(&end, NULL);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s->igrowths);
    assert(LOGSTORE_OK == logstore_close(&s));
}

void benchmark_puts_sync_every_put_int_value() {
    printf("%s: this might take a while...\n", __FUNCTION__);
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        logstore_id id;
        assert(LOGSTORE_OK == logstore_genid(s, &id));
        assert(LOGSTORE_OK == logstore_put(s, id, &i, sizeof(int), 0));
        assert(LOGSTORE_OK == logstore_sync(s));
    }
    gettimeofday(&end, NULL);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s->igrowths);
    assert(LOGSTORE_OK == logstore_close(&s));
}

void benchmark_puts_sync_once_per_second_int_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end, second_start, now;
    gettimeofday(&start, NULL);
    gettimeofday(&second_start, NULL);
    int sync_count = 0;
    for (int i=0; i<PUT_COUNT; ++i) {
        logstore_id id;
        assert(LOGSTORE_OK == logstore_genid(s, &id));
        assert(LOGSTORE_OK == logstore_put(s, id, &i, sizeof(int), 0));
        gettimeofday(&now, NULL);
        if (TIME_DELTA_SECONDS(second_start, now) >= 1) {
            assert(LOGSTORE_OK == logstore_sync(s));
            gettimeofday(&second_start, NULL);
            sync_count++;
        }
    }
    gettimeofday(&end, NULL);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d syncs performed\n", __FUNCTION__, sync_count);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s->igrowths);
    assert(LOGSTORE_OK == logstore_close(&s));
}

uint64_t first_put_1KiB_id = 0;

void benchmark_puts_no_sync_1KiB_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end; 
    char *data = malloc(1024);
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        logstore_id id;
        assert(LOGSTORE_OK == logstore_genid(s, &id));
        assert(LOGSTORE_OK == logstore_put(s, id, data, 1024, 0));
        if (i == 0) first_put_1KiB_id = id;
    }
    gettimeofday(&end, NULL);
    free(data);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s->igrowths);
    assert(LOGSTORE_OK == logstore_close(&s));
}

void benchmark_puts_sync_once_per_second_1KiB_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end, second_start, now;
    char *data = malloc(1024);
    gettimeofday(&start, NULL);
    gettimeofday(&second_start, NULL);
    int sync_count = 0;
    for (int i=0; i<PUT_COUNT; ++i) {
        logstore_id id;
        assert(LOGSTORE_OK == logstore_genid(s, &id));
        assert(LOGSTORE_OK == logstore_put(s, id, data, 1024, 0));
        gettimeofday(&now, NULL);
        if (TIME_DELTA_SECONDS(second_start, now) >= 1) {
            assert(LOGSTORE_OK == logstore_sync(s));
            gettimeofday(&second_start, NULL);
            sync_count++;
        }
    }
    gettimeofday(&end, NULL);
    free(data);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d syncs performed\n", __FUNCTION__, sync_count);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s->igrowths);
    assert(LOGSTORE_OK == logstore_close(&s));
}

void benchmark_sequential_gets_int_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        void *data = NULL;
        size_t sz = 0;
        assert(LOGSTORE_OK == logstore_get(s, first_put_int_id + i, &data, &sz, NULL));
        assert(NULL != data);
        assert(sz == sizeof(int));
        assert(*(int *)data == i);
        free(data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(LOGSTORE_OK == logstore_close(&s));
}

void benchmark_random_gets_int_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    srand(time(NULL));
    for (int i=0; i<PUT_COUNT; ++i) {
        uint64_t random_id = (rand() / (double)RAND_MAX) * PUT_COUNT;
        void *data = NULL;
        size_t sz = 0;
        assert(LOGSTORE_OK == logstore_get(s, first_put_int_id + random_id, &data, &sz, NULL));
        assert(NULL != data);
        assert(sz == sizeof(int));
        assert(*(int *)data == random_id);
        free(data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(LOGSTORE_OK == logstore_close(&s));
}

void benchmark_sequential_gets_1KiB_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        void *data = NULL;
        size_t sz = 0;
        assert(LOGSTORE_OK == logstore_get(s, first_put_1KiB_id + i, &data, &sz, NULL));
        assert(NULL != data);
        assert(sz == 1024);
        free(data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(LOGSTORE_OK == logstore_close(&s));
}

// Random gets perform a read from the index file (hopefully memory-mapped) and
// then a seek and read from the log file.  Thus, this is a random file access
// seek+read and is thus limited to the average seek time of the disk.  Thus,
// each read might take 5-20 ms which is an eternity.  However, the
// recommendation is to cache the read value/entry over time, amortizing the
// cost of the read over the lifetime of the "object".  This system is thus
// targeted to long-lived services like network servers.  Consider a user
// object.  Upon login, the user value is read which takes "forever".  Now, the
// value is deserialized as some higher-level "object", anything that gives
// meaning to the entry/blob.  Since the user logged-in, we expect the user to
// do things in the near future that would otherwise require the user object to
// be fetched multiple times.  Hence, "temporal locality" is exploited by way
// of caching the object.  Changes to the user object are "put" to the log
// logstore as appropriate.

void benchmark_random_gets_1KiB_value() {
    logstore s = NULL;
    assert(LOGSTORE_OK == logstore_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    srand(time(NULL));
    for (int i=0; i<1000; ++i) {        // limit this so it finishes :)
        uint64_t random_id = (rand() / (double)RAND_MAX) * 1000;
        void *data = NULL;
        size_t sz = 0;
        assert(LOGSTORE_OK == logstore_get(s, first_put_1KiB_id + random_id, &data, &sz, NULL));
        assert(NULL != data);
        assert(sz == 1024);
        free(data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = 1000 / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(LOGSTORE_OK == logstore_close(&s));
}

int main(int argc, char **argv) {
    unlink("log");
    unlink("log-index");
    benchmark_puts_no_sync_int_value();
    // VERY slow on mac os x at least.
    //benchmark_puts_sync_every_put_int_value();
    benchmark_puts_sync_once_per_second_int_value();
    benchmark_puts_no_sync_1KiB_value();
    benchmark_puts_sync_once_per_second_1KiB_value();
    benchmark_sequential_gets_int_value();
    benchmark_random_gets_int_value();
    benchmark_sequential_gets_1KiB_value();
    benchmark_random_gets_1KiB_value();
    return 0;
}
