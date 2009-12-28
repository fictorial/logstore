#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <assert.h>
#include <sys/time.h> 
#include <unistd.h>
#include "store.h"

#define ENTRY_COUNT 1000

// The log size is initially 0 and there are no entries in the corresponding
// index file.  index file should be grown to non-zero size upon creating a new
// log.  we should be able to map a small index file into memory.

void test_open_new_log() {
    puts(__FUNCTION__);
    unlink("log");
    unlink("log__index");
    struct store s;
    assert(0 == store_open(&s, "log"));
    assert(s.ifd > 2); // [0,2] stdin/out/err
    assert(s.lfd > 2);
    assert(s.lsz == 0);
    assert(s.icount == 0);
    assert(s.icap > 0);
    assert(s.imm != NULL);
    assert(s.imm_sz > 0);
    assert(0 == store_close(&s));
    puts("test_open_new_log: PASS");
}

void test_open_existing_but_empty_log() {
    puts(__FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    assert(s.ifd > 2); // [0,2] stdin/out/err
    assert(s.lfd > 2);
    assert(s.lsz == 0);
    assert(s.icount == 0);
    assert(s.icap > 0);
    assert(s.imm != NULL);
    assert(s.imm_sz > 0);
    assert(0 == store_close(&s));
    puts("test_open_existing_but_empty_log: PASS");
}

void test_id_generation() {
    puts(__FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v;
        assert(0 == store_genid(&s, &v));
        assert(v.id == i);
    }
    assert(0 == store_close(&s));
    puts("test_id_generation: PASS");
}

void test_put() {
    puts(__FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(&i, sizeof(int));
        v.id = i;
        assert(0 == store_put(&s, &v));
    }
    struct stat lst;
    assert(fstat(s.lfd, &lst) != -1);
    assert(lst.st_size == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    assert(0 == store_close(&s));
    puts("test_put: PASS");
}

void test_open_existing_non_empty_log() {
    puts(__FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    assert(s.ifd > 2); // [0,2] stdin/out/err
    assert(s.lfd > 2);
    assert(s.lsz == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    assert(s.icount == ENTRY_COUNT);
    assert(s.icap >= ENTRY_COUNT);
    assert(s.imm != NULL);
    assert(s.imm_sz > 0);
    assert(0 == store_close(&s));
    puts("test_open_existing_non_empty_log: PASS");
}

void test_get() {
    puts(__FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v = STORED_GET_INITIALIZER(i);
        assert(0 == store_get(&s, &v));
        assert(v.sz == sizeof(int));
        assert(*(int *)v.data == i);
        free(v.data);
    }
    assert(0 == store_close(&s));
    puts("test_get: PASS");
}

// Get 2 copies of entry with ID 0.  Put one of the copies back so its revision
// gets updated.  This should increment 'a.rev' given a successful put.  Then
// put 'b' which should fail due to a conflict as 'b' is out of date w.r.t. the
// 'latest' revision of entry with ID 0.

void test_confict_detection() {
    puts(__FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct stored a = STORED_GET_INITIALIZER(0);
    assert(0 == store_get(&s, &a));
    struct stored b = STORED_GET_INITIALIZER(0);
    assert(0 == store_get(&s, &b));
    assert(0 == store_put(&s, &a));
    assert(a.rev == b.rev + 1);    
    assert(store_put(&s, &b));
    assert(0 == store_close(&s));
    puts("test_confict_detection: PASS");
}

void test_remove() {
    puts(__FUNCTION__);
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct stored v = STORED_GET_INITIALIZER(0);
    assert(0 == store_get(&s, &v));   // exists from earlier tests
    assert(0 == store_rm(&s, &v));
    assert(1 == store_get(&s, &v));   // gone
    v.id = (uint64_t) -1;
    assert(1 == store_rm(&s, &v));    // way beyond any existing entry's ID
    assert(0 == store_close(&s));
    puts("test_remove: PASS");
}

#define TIME_DELTA_MICRO(start, end) \
  (((double)end.tv_sec   * 1000000.0 + (double)end.tv_usec) \
 - ((double)start.tv_sec * 1000000.0 + (double)start.tv_usec)) 

#define TIME_DELTA_SECONDS(start, end) (TIME_DELTA_MICRO(start, end) / 1e6)

#define PUT_COUNT 2000000

void benchmark_puts_no_sync_int_value() {
    struct store s;
    assert(0 == store_open(&s, "log"));
    struct timeval start, end; 
    gettimeofday(&start, NULL);
    for (int i=0; i<PUT_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(&i, sizeof(int));
        assert(0 == store_genid(&s, &v));
        assert(0 == store_put(&s, &v));
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

int main(int argc, char **argv) {
    unlink("log");
    unlink("log__index");
    test_open_new_log();
    test_open_existing_but_empty_log();
    test_id_generation();
    test_put();
    test_open_existing_non_empty_log();
    test_get();
    test_confict_detection();
    test_remove();
    puts("all tests passed");
    benchmark_puts_no_sync_int_value();
    // VERY slow on mac os x at least.
    //benchmark_puts_sync_every_put_int_value();
    benchmark_puts_sync_once_per_second_int_value();
    benchmark_puts_no_sync_1KiB_value();
    benchmark_puts_sync_once_per_second_1KiB_value();
    return 0;
}
