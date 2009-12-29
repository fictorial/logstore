#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h> 
#include <unistd.h>
#include "store.h"

#define ENTRY_COUNT 1000

int failures = 0, tests = 0;

#define CHECK(expr) \
    if (!(expr)) { \
        printf("%c[5;31m**FAILURE**%c[0;m ", 27, 27); \
        printf(#expr " (%s:%s:%d)\n", __FILE__, __FUNCTION__, __LINE__); \
        ++failures; \
    } \
    ++tests;

// The log size is initially 0 and there are no entries in the corresponding
// index file.  index file should be grown to non-zero size upon creating a new
// log.  we should be able to map a small index file into memory.

void test_open_new_log() {
    unlink("log");
    unlink("log__index");
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    CHECK(s.ifd > 2); // [0,2] stdin/out/err
    CHECK(s.lfd > 2);
    CHECK(s.lsz == 0);
    CHECK(s.icount == 0);
    CHECK(s.icap > 0);
    CHECK(s.imm != NULL);
    CHECK(s.imm_sz > 0);
    CHECK(STORE_OK == store_close(&s));
}

void test_open_existing_but_empty_log() {
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    CHECK(s.ifd > 2); // [0,2] stdin/out/err
    CHECK(s.lfd > 2);
    CHECK(s.lsz == 0);
    CHECK(s.icount == 0);
    CHECK(s.icap > 0);
    CHECK(s.imm != NULL);
    CHECK(s.imm_sz > 0);
    CHECK(STORE_OK == store_close(&s));
}

void test_id_generation() {
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v;
        CHECK(STORE_OK == store_genid(&s, &v));
        CHECK(v.id == i);
    }
    CHECK(STORE_OK == store_close(&s));
}

void test_put() {
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(&i, sizeof(int));
        v.id = i;
        CHECK(STORE_OK == store_put(&s, &v));
    }
    struct stat lst;
    CHECK(fstat(s.lfd, &lst) != -1);
    CHECK(lst.st_size == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    CHECK(STORE_OK == store_close(&s));
}

void test_open_existing_non_empty_log() {
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    CHECK(s.ifd > 2); // [0,2] stdin/out/err
    CHECK(s.lfd > 2);
    CHECK(s.lsz == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    CHECK(s.icount == ENTRY_COUNT);
    CHECK(s.icap >= ENTRY_COUNT);
    CHECK(s.imm != NULL);
    CHECK(s.imm_sz > 0);
    CHECK(STORE_OK == store_close(&s));
}

void test_get() {
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v = STORED_GET_INITIALIZER(i);
        CHECK(STORE_OK == store_get(&s, &v));
        CHECK(v.sz == sizeof(int));
        CHECK(*(int *)v.data == i);
        free(v.data);
    }
    CHECK(STORE_OK == store_close(&s));
}

// Get 2 copies of entry with ID 0.  Put one of the copies back so its revision
// gets updated.  This should increment 'a.rev' given a successful put.  Then
// put 'b' which should fail due to a conflict as 'b' is out of date w.r.t. the
// 'latest' revision of entry with ID 0.

void test_confict_detection() {
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    struct stored a = STORED_GET_INITIALIZER(0);
    CHECK(STORE_OK == store_get(&s, &a));
    struct stored b = STORED_GET_INITIALIZER(0);
    CHECK(STORE_OK == store_get(&s, &b));
    CHECK(STORE_OK == store_put(&s, &a));
    CHECK(a.rev == b.rev + 1);    
    CHECK(store_put(&s, &b));
    CHECK(STORE_OK == store_close(&s));
}

void test_remove() {
    struct store s;
    CHECK(STORE_OK == store_open(&s, "log"));
    struct stored v = STORED_GET_INITIALIZER(0);
    CHECK(STORE_OK == store_get(&s, &v));       // exists from earlier tests
    CHECK(STORE_OK == store_rm(&s, &v));
    CHECK(STORE_ENOENT == store_get(&s, &v));   // removed
    v.id = (uint64_t) -1;
    CHECK(STORE_EINVAL == store_rm(&s, &v));    // way beyond any existing entry's ID
    CHECK(STORE_OK == store_close(&s));
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
    printf("%c[5;33m%d tests, %d failure(s)%c[0;m\n", 27, tests, failures, 27);
    return failures;
}
