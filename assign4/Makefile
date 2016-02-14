CC=gcc
CFLAGS=-I.
OBJ=test_assign3_1.o storage_mgr.o dberror.o page_replacement.o buffer_mgr.o buffer_mgr_stat.o hashTable.o expr.o record_mgr.o rm_serializer.o
HEADERS=dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h dt.h hashTable.h page_replacement.h expr.h record_mgr.h tables.h

%.o: %.c $(HEADERS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign3_1: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS)

#test_assign1_1.o: test_assign1_1.c dberror.h storage_mgr.h test_helper.h
#	$(CC) -o test_assign1_1.o test_assign1_1.c $(CFLAGS)

#storage_mgr.o: storage_mgr.c storage_mgr.h dberror.h
#	$(CC) -o storage_mgr.o storage_mgr.c $(CFLAGS)

#dberror.o: dberror.c dberror.h
#	$(CC) -o dberror.o dberror.c $(CFLAGS)

clean:
	rm -f test_assign3_1 $(OBJ)

run: test_assign3_1
	./test_assign3_1
