PREFIX = /usr
VERSION = 0.1
TEST_EVENT = test-event
TEST_THREADS = test-threads
TEST_CODEC = test-codec
TEST_UCX = test-ucx
UCX_CFLAGS = $(shell pkg-config --cflags ucx)
UCX_LIBS = $(shell pkg-config --libs ucx)
CFLAGS = -fPIC -Wall -g -O0 -I .. -I ../../include -I ../../thirdparty/json-c/build/include -D_GNU_SOURCE -Wshadow -Wformat=2 -Wwrite-strings -fstack-protector-strong -Wnull-dereference -Wunreachable-code
FMT = clang-format-19

# default gcc, or use clang on 'make LLVM=1'
CC = gcc
ifneq ($(LLVM),)
CC = clang
else
CFLAGS += -Wduplicated-branches -Wrestrict
endif

__OS_TYPE = $(shell sh ../../scripts/utils/get_os_type.sh)
LIBJSONC_DIR = ../../thirdparty/json-c
include ../libjsonc.mk

.PHONY: all valgrind rebuild clean format

all: $(TEST_EVENT) $(TEST_THREADS) $(TEST_CODEC) $(TEST_UCX)

$(TEST_EVENT):
	$(CC) test_event.c ../event.c $(CFLAGS) -o $(TEST_EVENT) -lpthread

$(TEST_THREADS):
	$(CC) test_threads.c ../workqueue.c ../threads.c ../event.c ../log.c $(CFLAGS) -o $(TEST_THREADS) -lpthread

$(TEST_CODEC): test_codec.c ../codec.c ../../lib/log.c $(LIBJSONC)
	$(CC) $^ $(CFLAGS) -o $(TEST_CODEC)

$(TEST_UCX): test_ucx.c ../ucx.c ../../lib/log.c
	$(CC) $^ $(CFLAGS) $(UCX_CFLAGS) -o $(TEST_UCX) $(UCX_LIBS)

valgrind: $(TEST_EVENT) $(TEST_THREADS)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_EVENT)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_THREADS)
	valgrind -s --track-origins=yes --show-possibly-lost=no --leak-check=full ./$(TEST_CODEC)

rebuild: clean
	make all

clean:
	rm -f $(TEST_EVENT) $(TEST_THREADS) $(TEST_CODEC) $(TEST_UCX)

format:
	$(FMT) -i *.c
