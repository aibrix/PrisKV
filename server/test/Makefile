PREFIX = /usr
VERSION = 0.1
TEST_BUDDY = test-buddy
TEST_BUDDY_MT = test-buddy-mt
TEST_SLAB = test-slab
TEST_SLAB_MT = test-slab-mt
TEST_KV = test-kv
TEST_KV_MT = test-kv-mt
TEST_MEMORY = test-memory
TEST_ACL = test-acl
TEST_KV_EXPIRE_ROUTINE = test-kv-expire-routine
TEST_BE_REDIS = test-be-redis
CFLAGS = -fPIC -Wall -g -O0 -I .. -I ../../include -D_GNU_SOURCE -Wshadow -Wformat=2 -Wwrite-strings -fstack-protector-strong -Wnull-dereference -Wunreachable-code -lpthread
FMT = clang-format-19

# default gcc, or use clang on 'make LLVM=1'
CC = gcc
ifneq ($(LLVM),)
CC = clang
else
CFLAGS += -Wduplicated-branches -Wrestrict
endif

.PHONY: $(TEST_BUDDY) ${TEST_BUDDY_MT} $(TEST_SLAB) $(TEST_SLAB_MT) $(TEST_KV) $(TEST_KV_MT) $(TEST_MEMORY) $(TEST_ACL) $(TEST_KV_EXPIRE_ROUTINE) $(TEST_BE_REDIS)
OBJS = ../memory.o ../kv.o ../slab.o ../crc.o ../acl.o

all: $(TEST_BUDDY) ${TEST_BUDDY_MT} $(TEST_SLAB) $(TEST_SLAB_MT) $(TEST_KV) $(TEST_KV_MT) $(TEST_MEMORY) $(TEST_ACL) $(TEST_KV_EXPIRE_ROUTINE) $(TEST_BE_REDIS)

$(TEST_BUDDY): $(OBJS)
	$(CC) test_buddy.c ../buddy.c $(CFLAGS) -o $(TEST_BUDDY)
$(TEST_BUDDY_MT): $(OBJS)
	$(CC) test_buddy_mt.c ../buddy.c $(CFLAGS) -pthread -o $(TEST_BUDDY_MT)

$(TEST_SLAB): $(OBJS)
	$(CC) test_slab.c ../slab.c $(CFLAGS) -o $(TEST_SLAB)
$(TEST_SLAB_MT): $(OBJS)
	$(CC) test_slab_mt.c ../slab.c $(CFLAGS) -pthread -o $(TEST_SLAB_MT)

UCX_CFLAGS = $(shell pkg-config --cflags ucx)
UCX_LIBS = $(shell pkg-config --libs ucx)
ifeq ($(shell pkg-config --exists ucx; echo $$?),0)
$(TEST_KV): $(OBJS)
	$(CC) test_kv.c ../../lib/workqueue.c ../../lib/threads.c ../../lib/event.c ../../lib/ucx.c ../../lib/config.c ../memory.c ../kv.c ../slab.c ../buddy.c ../crc.c ../../lib/log.c ../backend/backend.c ../transport/transport.c ../transport/rdma.c ../transport/ucx.c ../tiering.c ../acl.c $(CFLAGS) $(UCX_CFLAGS) -o $(TEST_KV) -lmount -lrdmacm -libverbs $(UCX_LIBS)
else
$(TEST_KV):
	@echo "UCX not available, skip $(TEST_KV)"
endif

ifeq ($(shell pkg-config --exists ucx; echo $$?),0)
$(TEST_KV_MT): $(OBJS)
	$(CC) test_kv_mt.c ../../lib/workqueue.c ../../lib/threads.c ../../lib/event.c ../../lib/ucx.c ../../lib/config.c ../memory.c ../kv.c ../slab.c ../buddy.c ../crc.c ../../lib/log.c ../backend/backend.c ../transport/transport.c ../transport/rdma.c ../transport/ucx.c ../tiering.c ../acl.c $(CFLAGS) $(UCX_CFLAGS) -o $(TEST_KV_MT) -lmount -lrdmacm -libverbs $(UCX_LIBS)
else
$(TEST_KV_MT):
	@echo "UCX not available, skip $(TEST_KV_MT)"
endif

$(TEST_MEMORY): $(OBJS)
	$(CC) test_memory.c ../memory.c ../../lib/log.c $(CFLAGS) -lmount -o $(TEST_MEMORY)

$(TEST_ACL): $(OBJS)
	$(CC) test_acl.c ../acl.c ../../lib/log.c $(CFLAGS) -lrdmacm -o $(TEST_ACL)

ifeq ($(shell pkg-config --exists ucx; echo $$?),0)
$(TEST_KV_EXPIRE_ROUTINE): $(OBJS)
	$(CC) test_kv_expire_routine.c ../../lib/workqueue.c ../../lib/threads.c ../../lib/event.c ../../lib/ucx.c ../../lib/config.c ../memory.c ../kv.c ../slab.c ../buddy.c ../crc.c ../../lib/log.c ../backend/backend.c ../transport/transport.c ../transport/rdma.c ../transport/ucx.c ../tiering.c ../acl.c $(CFLAGS) $(UCX_CFLAGS) -o $(TEST_KV_EXPIRE_ROUTINE) -lmount -lpthread -lrdmacm -libverbs $(UCX_LIBS)
else
$(TEST_KV_EXPIRE_ROUTINE):
	@echo "UCX not available, skip $(TEST_KV_EXPIRE_ROUTINE)"
endif

$(TEST_BE_REDIS):
	$(CC) test_be_redis.c ../../lib/log.c ../../lib/event.c ../../lib/workqueue.c ../../lib/threads.c ../backend/backend.c ../backend/be_redis.c $(CFLAGS) -o $(TEST_BE_REDIS) -levent -lhiredis

valgrind: $(TEST_BUDDY) $(TEST_BUDDY_MT) $(TEST_SLAB) $(TEST_SLAB_MT) $(TEST_KV) $(TEST_KV_MT) $(TEST_MEMORY) $(TEST_ACL) $(TEST_KV_EXPIRE_ROUTINE)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_BUDDY)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_BUDDY_MT)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_SLAB)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_SLAB_MT)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_KV)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_KV_MT)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_MEMORY)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_ACL)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_KV_EXPIRE_ROUTINE)

rebuild: clean
	make all

clean:
	rm -f *.o *.d
	rm -f $(TEST_BUDDY) $(TEST_BUDDY_MT) $(TEST_SLAB) $(TEST_KV) $(TST_KV_MT) $(TEST_SLAB_MT) $(TEST_MEMORY) $(TEST_KV_MT) $(TEST_ACL) $(TEST_KV_EXPIRE_ROUTINE)

format:
	$(FMT) -i *.c
