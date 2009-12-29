#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/time.h> 
#include <unistd.h>
#include "store.h"

#define TIME_DELTA_MICRO(start, end) \
  (((double)end.tv_sec   * 1000000.0 + (double)end.tv_usec) \
 - ((double)start.tv_sec * 1000000.0 + (double)start.tv_usec)) 

#define TIME_DELTA_SECONDS(start, end) (TIME_DELTA_MICRO(start, end) / 1e6)

#define PUT_COUNT 2000000

uint64_t first_put_int_id = 0;

void benchmark_puts_no_sync_int_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(&i, sizeof(int));
        assert(0 == store_genid(&s, &v));
        assert(0 == store_put(&s, &v));
        if (i == 0) first_put_int_id = v.id;
    }
    gettimeofday(&end, NULL);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s.igrowths);
    assert(0 == store_close(&s));
}

void benchmark_puts_sync_every_put_int_value() {
    printf("%s: this might take a while...\n", __FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(&i, sizeof(int));
        assert(0 == store_genid(&s, &v));
        assert(0 == store_put(&s, &v));
        assert(0 == store_sync(&s));
    }
    gettimeofday(&end, NULL);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s.igrowths);
    assert(0 == store_close(&s));
}

void benchmark_puts_sync_once_per_second_int_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end, second_start, now;
    gettimeofday(&start, NULL);
    gettimeofday(&second_start, NULL);
    int sync_count = 0;
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(&i, sizeof(int));
        assert(0 == store_genid(&s, &v));
        assert(0 == store_put(&s, &v));
        gettimeofday(&now, NULL);
        if (TIME_DELTA_SECONDS(second_start, now) >= 1) {
            assert(0 == store_sync(&s));
            gettimeofday(&second_start, NULL);
            sync_count++;
        }
    }
    gettimeofday(&end, NULL);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d syncs performed\n", __FUNCTION__, sync_count);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s.igrowths);
    assert(0 == store_close(&s));
}

uint64_t first_put_1KiB_id = 0;

void benchmark_puts_no_sync_1KiB_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    char *data = malloc(1024);
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(data, 1024);
        assert(0 == store_genid(&s, &v));
        assert(0 == store_put(&s, &v));
        if (i == 0) first_put_1KiB_id = v.id;
    }
    gettimeofday(&end, NULL);
    free(data);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s.igrowths);
    assert(0 == store_close(&s));
}

void benchmark_puts_sync_once_per_second_1KiB_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end, second_start, now;
    char *data = malloc(1024);
    gettimeofday(&start, NULL);
    gettimeofday(&second_start, NULL);
    int sync_count = 0;
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(data, 1024);
        assert(0 == store_genid(&s, &v));
        assert(0 == store_put(&s, &v));
        gettimeofday(&now, NULL);
        if (TIME_DELTA_SECONDS(second_start, now) >= 1) {
            assert(0 == store_sync(&s));
            gettimeofday(&second_start, NULL);
            sync_count++;
        }
    }
    gettimeofday(&end, NULL);
    free(data);
    double puts_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u puts / second\n", __FUNCTION__, (unsigned)puts_per_second);
    printf("%s: %d syncs performed\n", __FUNCTION__, sync_count);
    printf("%s: %d index file growths performed\n", __FUNCTION__, s.igrowths);
    assert(0 == store_close(&s));
}

void benchmark_sequential_gets_int_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_GET_INITIALIZER(first_put_int_id + i);
        assert(0 == store_get(&s, &v));
        assert(NULL != v.data);
        assert(v.sz == sizeof(int));
        assert(*(int *)v.data == i);
        free(v.data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(0 == store_close(&s));
}

void benchmark_random_gets_int_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    srand(time(NULL));
    for (int i=0; i<PUT_COUNT; ++i) {
        uint64_t random_id = arc4random() % PUT_COUNT;
        struct stored v = STORED_GET_INITIALIZER(first_put_int_id + random_id);
        assert(0 == store_get(&s, &v));
        assert(NULL != v.data);
        assert(v.sz == sizeof(int));
        assert(*(int *)v.data == random_id);
        free(v.data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(0 == store_close(&s));
}

void benchmark_sequential_gets_1KiB_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_GET_INITIALIZER(first_put_1KiB_id + i);
        assert(0 == store_get(&s, &v));
        assert(NULL != v.data);
        assert(v.sz == 1024);
        free(v.data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = PUT_COUNT / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(0 == store_close(&s));
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
// store as appropriate.

void benchmark_random_gets_1KiB_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    srand(time(NULL));
    for (int i=0; i<1000; ++i) {        // limit this so it finishes :)
        uint64_t random_id = arc4random() % 1000;
        struct stored v = STORED_GET_INITIALIZER(first_put_1KiB_id + random_id);
        assert(0 == store_get(&s, &v));
        assert(NULL != v.data);
        assert(v.sz == 1024);
        free(v.data);
    }
    gettimeofday(&end, NULL);
    double gets_per_second = 1000 / TIME_DELTA_SECONDS(start, end);
    printf("%s: %u gets / second\n", __FUNCTION__, (unsigned)gets_per_second);
    assert(0 == store_close(&s));
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
