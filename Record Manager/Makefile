CC=gcc

CFLAGS=-lpthread -std=gnu99 -I.

DEPS=buffer_mgr.h \
	buffer_mgr_stat.h \
	dberror.h \
	dt.h \
	expr.h \
	record_mgr.h \
	storage_mgr.h \
	tables.h \
	test_helper.h

OBJ1=storage_mgr.o \
	dberror.o

OBJ2=storage_mgr.o \
	buffer_mgr_stat.o \
	buffer_mgr.o \
	dberror.o

OBJ3=buffer_mgr.o \
	buffer_mgr_stat.o \
	dberror.o \
	expr.o \
	record_mgr.o \
	rm_serializer.o \
	storage_mgr.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

all: test_assign3_1 \
	 test_expr

buffer_mgr.o: buffer_mgr.c
	$(CC) -c buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c
	$(CC) -c buffer_mgr_stat.c

dberror.o: dberror.c
	$(CC) -c dberror.c

expr.o: expr.c
	$(CC) -c expr.c

record_mgr.o: record_mgr.c
	$(CC) -c record_mgr.c

rm_serializer.o: rm_serializer.c
	$(CC) -c rm_serializer.c

storage_mgr.o: storage_mgr.c
	$(CC) -c storage_mgr.c


# assign 3
# test_assign3_1
test_assign3_1.o: test_assign3_1.c
	$(CC) -c test_assign3_1.c

test_assign3_1: $(OBJ3)	test_assign3_1.o
	$(CC) -o $@ $^ $(CFLAGS)



# test_expr
test_expr.o: test_expr.c
	$(CC) -c test_expr.c

test_expr: $(OBJ3)	test_expr.o
	$(CC) -o $@ $^ $(CFLAGS)

.PHONY: clean
clean:
	-rm -f test_assign3_1 test_expr *.o *.log

