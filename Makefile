all: lib 

lib: libstore.a

libstore.a: store.o
	ar rcs libstore.a store.o
	ranlib libstore.a

store.o: store.c store.h
	gcc -g -c store.c -std=c99

test_store: test_store.c libstore.a
	rm -f log log__index
	gcc -o test_store -std=c99 -g test_store.c -L. -lstore
	./test_store

check: test_store

test: test_store

bench_store: bench_store.c libstore.a
	rm -f log log__index
	gcc -o bench_store -std=c99 -g bench_store.c -L. -lstore
	./bench_store

bench: bench_store

benchmark: bench_store

clean:
	rm -rf test_store libstore.a store.o log log__index bench_store *.dSYM

.PHONY: all lib test clean check bench benchmark
