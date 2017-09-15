CC = gcc
CFLAGS  = -w -std=gnu99
 
default: test_assign4_1

test_assign4_1: test_assign4_1.o btree_mgr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test_assign4_1 test_assign4_1.o btree_mgr.o dberror.o expr.o record_mgr.o rm_serializer.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o 
	
test_assign4_1.o: test_assign4_1.c dberror.h expr.h record_mgr.h tables.h test_helper.h btree_mgr.h buffer_mgr.h
	$(CC) $(CFLAGS) -c test_assign4_1.c -lm

btree_mgr.o: btree_mgr.c dberror.h expr.h record_mgr.h tables.h test_helper.h btree_mgr.h
	$(CC) $(CFLAGS) -c btree_mgr.c
	
record_mgr.o: record_mgr.c record_mgr.h buffer_mgr.h storage_mgr.h
	$(CC) $(CFLAGS) -c  record_mgr.c

expr.o: expr.c dberror.h record_mgr.h expr.h tables.h
	$(CC) $(CFLAGS) -c expr.c

rm_serializer.o: rm_serializer.c dberror.h tables.h record_mgr.h
	$(CC) $(CFLAGS) -c rm_serializer.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr.c

storage_mgr.o: storage_mgr.c storage_mgr.h 
	$(CC) $(CFLAGS) -c storage_mgr.c -lm

dberror.o: dberror.c dberror.h 
	$(CC) $(CFLAGS) -c dberror.c

clean: 
	$(RM) test_assign4_1 *.o *~ *.log 

run:
	./test_assign4_1 > t1.log