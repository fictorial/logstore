#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h> 
#include <unistd.h>
#include <assert.h>
#include <stdint.h>

#include "store_private.h"
#include "store.h"


#define ENTRY_COUNT 1000

// The log size is initially 0 and there are no entries in the corresponding
// index file.  index file should be grown to non-zero size upon creating a new
// log.  we should be able to map a small index file into memory.

void test_open_new_log() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    assert(s->ifd > 2); // [0,2] stdin/out/err
    assert(s->lfd > 2);
    assert(s->lsz == 0);
    assert(s->icount == 0);
    assert(s->icap > 0);
    assert(s->imm != NULL);
    assert(s->imm_sz > 0);
    assert(STORE_OK == store_close(&s));
}

void test_open_existing_but_empty_log() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    assert(s->ifd > 2); // [0,2] stdin/out/err
    assert(s->lfd > 2);
    assert(s->lsz == 0);
    assert(s->icount == 0);
    assert(s->icap > 0);
    assert(s->imm != NULL);
    assert(s->imm_sz > 0);
    assert(STORE_OK == store_close(&s));
}

void test_id_generation() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        store_id id;
        assert(STORE_OK == store_genid(s, &id));
        assert(id == i);
    }
    assert(STORE_OK == store_close(&s));
}

void test_put() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) 
        assert(STORE_OK == store_put(s, i, &i, sizeof(i), 0));
    struct stat lst;
    assert(fstat(s->lfd, &lst) != -1);
    assert(lst.st_size == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    assert(STORE_OK == store_close(&s));
}

void test_open_existing_non_empty_log() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    assert(s->ifd > 2); // [0,2] stdin/out/err
    assert(s->lfd > 2);
    assert(s->lsz == ENTRY_COUNT * (sizeof(uint64_t)*2 + sizeof(int)));
    assert(s->icount == ENTRY_COUNT);
    assert(s->icap >= ENTRY_COUNT);
    assert(s->imm != NULL);
    assert(s->imm_sz > 0);
    assert(STORE_OK == store_close(&s));
}

void test_get() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    for (int i=0; i<ENTRY_COUNT; ++i) {
        void *data = NULL;
        size_t sz = 0;
        assert(STORE_OK == store_get(s, i, &data, &sz, NULL));
        assert(sz == sizeof(int));
        assert(*(int *)data == i);
        free(data);
    }
    assert(STORE_OK == store_close(&s));
}

// Get 2 copies of entry with ID 0.  Put one of the copies back so its revision
// gets updated.  This should increment 'a.rev' given a successful put.  Then
// put 'b' which should fail due to a conflict as 'b' is out of date w.r.t. the
// 'latest' revision of entry with ID 0.

void test_confict_detection() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    void *data_a = NULL;
    void *data_b = NULL;
    size_t sz_a, sz_b;
    store_revision rev_a, rev_b;
    assert(STORE_OK == store_get(s, 0, &data_a, &sz_a, &rev_a));
    assert(STORE_OK == store_get(s, 0, &data_b, &sz_b, &rev_b));
    assert(STORE_OK == store_put(s, 0, data_a, sz_a, rev_a));
    // at this point the revision of entry with id 0 is 2
    assert(STORE_ECONFLICT == store_put(s, 0, data_b, sz_b, rev_b));
    assert(STORE_OK == store_close(&s));
    free(data_a);
    free(data_b);
}

void test_remove() {
    store s = NULL;
    assert(STORE_OK == store_open(&s, "log"));
    void *data = NULL;
    size_t sz;
    assert(STORE_OK == store_get(s, 0, &data, &sz, NULL));
    free(data);
    assert(STORE_OK == store_rm(s, 0));
    data = NULL;
    assert(STORE_ENOENT == store_get(s, 0, &data, NULL, NULL));   // removed!
    // way beyond any existing entry's ID:
    assert(STORE_EINVAL == store_rm(s, (uint64_t) -1));    
    assert(STORE_OK == store_close(&s));
}

int main(int argc, char **argv) {
    unlink("log");
    unlink("log-index");

    test_open_new_log();
    test_open_existing_but_empty_log();
    test_id_generation();
    test_put();
    test_open_existing_non_empty_log();
    test_get();
    test_confict_detection();
    test_remove();

    return 0;
}
