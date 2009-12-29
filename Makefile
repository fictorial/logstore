CFLAGS=-std=c99 -O2 -Wall -Werror -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64
LDFLAGS=-L. -lstore -pthread

all: lib 

lib: libstore.a

libstore.a: store.o
	ar rcs libstore.a store.o
	ranlib libstore.a

store.o: store.c store.h store_private.h
	gcc -c $(CFLAGS) store.c 

test_store: test_store.c libstore.a
	gcc $(CFLAGS) test_store.c -o test_store $(LDFLAGS) 

check: test_store
	./test_store

test: check

bench_store: bench_store.c libstore.a
	gcc $(CFLAGS) bench_store.c -o bench_store $(LDFLAGS) 

bench: bench_store
	./bench_store

benchmark: bench

clean:
	rm -rf test_store libstore.a store.o log log-index bench_store *.dSYM

.PHONY: all lib test clean check bench benchmark
