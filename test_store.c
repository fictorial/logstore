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
    assert(STORE_OK == store_open(&s, "log"));
    assert(s.ifd > 2); // [0,2] stdin/out/err
    assert(s.lfd > 2);
    assert(s.lsz == 0);
    assert(s.icount == 0);
    assert(s.icap > 0);
    assert(s.imm != NULL);
    assert(s.imm_sz > 0);
    assert(STORE_OK == store_close(&s));
    puts("test_open_new_log: PASS");
}

void test_open_existing_but_empty_log() {
    puts(__FUNCTION__);
    struct store s;
    assert(STORE_OK == store_open(&s, "log"));
    assert(s.ifd > 2); // [0,2] stdin/out/err
    assert(s.lfd > 2);
    assert(s.lsz == 0);
    assert(s.icount == 0);
    assert(s.icap > 0);
    assert(s.imm != NULL);
    assert(s.imm_sz > 0);
    assert(STORE_OK == store_close(&s));
    puts("test_open_existing_but_empty_log: PASS");
}

void test_id_generation() {
    puts(__FUNCTION__);
    struct store s;
    assert(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v;
        assert(STORE_OK == store_genid(&s, &v));
        assert(v.id == i);
    }
    assert(STORE_OK == store_close(&s));
    puts("test_id_generation: PASS");
}

void test_put() {
    puts(__FUNCTION__);
    struct store s;
    assert(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v = STORED_PUT_INITIALIZER(&i, sizeof(int));
        v.id = i;
        assert(STORE_OK == store_put(&s, &v));
    }
    struct stat lst;
    assert(fstat(s.lfd, &lst) != -1);
    assert(lst.st_size == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    assert(STORE_OK == store_close(&s));
    puts("test_put: PASS");
}

void test_open_existing_non_empty_log() {
    puts(__FUNCTION__);
    struct store s;
    assert(STORE_OK == store_open(&s, "log"));
    assert(s.ifd > 2); // [0,2] stdin/out/err
    assert(s.lfd > 2);
    assert(s.lsz == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    assert(s.icount == ENTRY_COUNT);
    assert(s.icap >= ENTRY_COUNT);
    assert(s.imm != NULL);
    assert(s.imm_sz > 0);
    assert(STORE_OK == store_close(&s));
    puts("test_open_existing_non_empty_log: PASS");
}

void test_get() {
    puts(__FUNCTION__);
    struct store s;
    assert(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        struct stored v = STORED_GET_INITIALIZER(i);
        assert(STORE_OK == store_get(&s, &v));
        assert(v.sz == sizeof(int));
        assert(*(int *)v.data == i);
        free(v.data);
    }
    assert(STORE_OK == store_close(&s));
    puts("test_get: PASS");
}

// Get 2 copies of entry with ID 0.  Put one of the copies back so its revision
// gets updated.  This should increment 'a.rev' given a successful put.  Then
// put 'b' which should fail due to a conflict as 'b' is out of date w.r.t. the
// 'latest' revision of entry with ID 0.

void test_confict_detection() {
    puts(__FUNCTION__);
    struct store s;
    assert(STORE_OK == store_open(&s, "log"));
    struct stored a = STORED_GET_INITIALIZER(0);
    assert(STORE_OK == store_get(&s, &a));
    struct stored b = STORED_GET_INITIALIZER(0);
    assert(STORE_OK == store_get(&s, &b));
    assert(STORE_OK == store_put(&s, &a));
    assert(a.rev == b.rev + 1);    
    assert(store_put(&s, &b));
    assert(STORE_OK == store_close(&s));
    puts("test_confict_detection: PASS");
}

void test_remove() {
    puts(__FUNCTION__);
    struct store s;
    assert(STORE_OK == store_open(&s, "log"));
    struct stored v = STORED_GET_INITIALIZER(0);
    assert(STORE_OK == store_get(&s, &v));       // exists from earlier tests
    assert(STORE_OK == store_rm(&s, &v));
    assert(STORE_ENOENT == store_get(&s, &v));   // gone
    v.id = (uint64_t) -1;
    assert(STORE_EINVAL == store_rm(&s, &v));    // way beyond any existing entry's ID
    assert(STORE_OK == store_close(&s));
    puts("test_remove: PASS");
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
    return 0;
}
