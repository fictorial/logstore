all: lib 

lib: libstore.a

libstore.a: store.o
	ar rcs libstore.a store.o
	ranlib libstore.a

store.o: store.c store.h
	gcc -g -c store.c -std=c99

test_store: test_store.o libstore.a
	gcc -o test_store test_store.o -L. -lstore -pthread

test_store.o: test_store.c store.h
	gcc -g -c test_store.c -std=c99

test: test_store
	@rm -f log log__index
	@./test_store

check: test

clean:
	rm -f test_store test_store.o libstore.a store.o log log__index

.PHONY: all lib test clean check
