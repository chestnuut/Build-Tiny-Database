CC=gcc
CFLAGS = -I
DEPS = buffer_mgr.h storage_mgr.h dberror.h buffer_mgr_stat.h dt.h test_helper.h

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $<

dberror.o: dberror.c
	cc -o dberror.o -c dberror.c -g -Wall

storage_mgr.o: storage_mgr.c
	cc -o storage_mgr.o -c storage_mgr.c -g -Wall

buffer_mgr.o: buffer_mgr.c
	cc -o buffer_mgr.o -c buffer_mgr.c -g -Wall

buffer_mgr_stat.o: buffer_mgr_stat.c
	cc -o buffer_mgr_stat.o -c buffer_mgr_stat.c -g -Wall

test_assign2_1.o: test_assign2_1.c
	cc -o test_assign2_1.o -c test_assign2_1.c -g -Wall

test_assign2_2.o: test_assign2_2.c
	cc -o test_assign2_2.o -c test_assign2_2.c -g -Wall

assign2_1: test_assign2_1.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o
	cc -o assign2_1 test_assign2_1.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o -I. -g -Wall

assign2_2: test_assign2_2.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o
	cc -o assign2_2 test_assign2_2.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o -I. -g -Wall

run: 
	./assign2_1 > assign2_1.log
	./assign2_2 > assign2_2.log

clean: 
	-rm -f assign2_1 assign2_2 *.o *.log *.bin
