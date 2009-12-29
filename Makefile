CFLAGS=-std=c99 -O2 -Wall -Werror -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64
LDFLAGS=-L. -llogstore -pthread

all: lib 

lib: liblogstore.a

liblogstore.a: logstore.o
	ar rcs liblogstore.a logstore.o
	ranlib liblogstore.a

logstore.o: logstore.c logstore.h logstore_private.h
	gcc -c $(CFLAGS) logstore.c 

test_logstore: test_logstore.c liblogstore.a
	gcc $(CFLAGS) test_logstore.c -o test_logstore $(LDFLAGS) 

check: test_logstore
	./test_logstore

test: check

bench_logstore: bench_logstore.c liblogstore.a
	gcc $(CFLAGS) bench_logstore.c -o bench_logstore $(LDFLAGS) 

bench: bench_logstore
	./bench_logstore

benchmark: bench

clean:
	rm -rf test_logstore liblogstore.a logstore.o log log-index bench_logstore *.dSYM

.PHONY: all lib test clean check bench benchmark
