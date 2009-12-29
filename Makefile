all: lib 

lib: libstore.a

libstore.a: store.o
	ar rcs libstore.a store.o
	ranlib libstore.a

store.o: store.c store.h
	gcc -c store.c -std=c99

test_store: test_store.c libstore.a
	gcc -o test_store -std=c99 -O2 test_store.c -L. -lstore -pthread

check: test_store
	./test_store

test: check

bench_store: bench_store.c libstore.a
	gcc -o bench_store -std=c99 -O2 bench_store.c -L. -lstore

bench: bench_store
	./bench_store

benchmark: bench

clean:
	rm -rf test_store libstore.a store.o log log-index bench_store *.dSYM

.PHONY: all lib test clean check bench benchmark
