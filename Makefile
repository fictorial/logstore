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
	rm -f log log__index
	./test_store

test: check

bench_store: bench_store.c libstore.a
	rm -f log log__index
	gcc -o bench_store -std=c99 -O2 bench_store.c -L. -lstore
	./bench_store

bench: bench_store

benchmark: bench_store

clean:
	rm -rf test_store libstore.a store.o log log__index bench_store *.dSYM

.PHONY: all lib test clean check bench benchmark
