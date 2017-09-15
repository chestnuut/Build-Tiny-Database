CC=gcc
DEPS = dberror.h storage_mgr.h test_helper.h
OBJ = dberror.o storage_mgr.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< 
all: assign1_1

assign1_1: $(OBJ) test_assign1_1.o
	$(CC) -o $@ $^

dberror.o: dberror.c
	$(CC) -c dberror.c
storage_mgr.o: storage_mgr.c
	$(CC) -c storage_mgr.c -std=c99
test_assign1_1.o: test_assign1_1.c
	$(CC) -c test_assign1_1.c

run:assign1_1
	./assign1_1 > a1_1.log

clean:
	-rm -f *.o *.log
