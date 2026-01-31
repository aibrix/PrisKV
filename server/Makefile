VERSION = 0.1
UCX_CFLAGS = $(shell pkg-config --cflags ucx)
UCX_LIBS = $(shell pkg-config --libs ucx)
LIBS = $(UCX_LIBS) -lrdmacm -libverbs -lpthread -lmount -levent -levent_openssl -lssl -lcrypto -luring -lhiredis -ldl
INCFLAGS = -I../include -I../client
CFLAGS = -fPIC -Wall -g $(INCFLAGS) -D_GNU_SOURCE -Wshadow -Wformat=2 -Wwrite-strings -fstack-protector-strong -Wnull-dereference -Wunreachable-code
FMT = clang-format-19
MKDIR = mkdir -p
__OS_TYPE = $(shell sh ../scripts/utils/get_os_type.sh)

DEPS_STATIC_LIBS = ../client/libpriskv.a

ifeq ($(__OS_TYPE), unknown)
$(error "Unknown OS type")
endif

# default gcc, or use clang on 'make LLVM=1'
CC = gcc
ifneq ($(LLVM),)
CC = clang
else
CFLAGS += -Wduplicated-branches -Wrestrict
endif

CUDA_HOME ?= /usr/local/cuda
CUDA_LIB ?= $(CUDA_HOME)/lib64
CUDA_INC ?= $(CUDA_HOME)/include
CUDARTLIB  ?= cudart_static
CUDA_LDFLAGS = -I$(CUDA_INC) -L$(CUDA_LIB) -l$(CUDARTLIB) -lcuda -ldl -lrt

ifneq (,$(filter $(PRISKV_USE_CUDA),yes YES y Y 1))
CFLAGS += $(CUDA_LDFLAGS) -DPRISKV_USE_CUDA
endif

ifneq ($(PRISKV_RELEASE),)
CFLAGS += -O2
else
CFLAGS += -O0
endif

PRISKV_BINPATH = usr/bin
PRISKV_MANPATH = usr/share/man

PRISKV_SERVER_TARGETS = priskv-server priskv-memfile
PRISKV_SERVER_TARGETS_SRCS = $(patsubst priskv-%, ./%.c, $(PRISKV_SERVER_TARGETS))
PRISKV_SERVER_SRCS = $(filter-out $(PRISKV_SERVER_TARGETS_SRCS), $(shell find . -path ./test -prune -o -name "*.c" -print))
PRISKV_SERVER_OBJS := $(PRISKV_SERVER_SRCS:%.c=%.o)
PRISKV_SERVER_DEPS := $(PRISKV_SERVER_OBJS:%.o=%.d)

COMMON_LIB_DIR = ../lib
COMMON_LIB_SRCS = $(foreach dir,$(COMMON_LIB_DIR),$(wildcard $(dir)/*.c))
COMMON_LIB_OBJS := $(COMMON_LIB_SRCS:%.c=%.o)
COMMON_LIB_DEPS := $(COMMON_LIB_OBJS:%.o=%.d)

.PHONY: all test rebuild clean format man install

# default target MUST be the first target
all: $(PRISKV_SERVER_TARGETS) test man

LIBJSONC_DIR = ../thirdparty/json-c
include ../lib/libjsonc.mk
include libevhtp.mk

ifeq ($(filter clean format, $(MAKECMDGOALS)),)
include $(PRISKV_SERVER_DEPS)
include $(COMMON_LIB_DEPS)
endif

%.d: %.c
	@rm -f $@; \
	$(CC) -MM -MG $< $(INCFLAGS) >$@.tmp; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.tmp > $@; \
	rm -f $@.tmp

$(PRISKV_SERVER_TARGETS): priskv-%: %.c $(PRISKV_SERVER_OBJS) $(COMMON_LIB_OBJS) $(DEPS_STATIC_LIBS)
	$(CC) $^ -o $@ $(CFLAGS) $(UCX_CFLAGS) $(LIBS)

MAN_DIR =../man
$(MAN_DIR)/priskv-server.1: $(MAN_DIR)/priskv-server.man
	sed 's/__PRISKV_VERSION__/$(shell date +%Y%m%d)/g' $< > $@
man: $(MAN_DIR)/priskv-server.1

install: $(PRISKV_SERVER_TARGETS) man
	install -m 755 -d $(PRISKV_DESTDIR)/$(PRISKV_BINPATH)
	install -m 755 $(PRISKV_SERVER_TARGETS) $(PRISKV_DESTDIR)/$(PRISKV_BINPATH)
	install -m 755 -d $(PRISKV_DESTDIR)/$(PRISKV_MANPATH)/man1
	install -m 755 $(MAN_DIR)/priskv-server.1 $(PRISKV_DESTDIR)/$(PRISKV_MANPATH)/man1

test:
	make -C test all

rebuild: clean
	make all

clean:
	make -C test clean
	rm -f $(PRISKV_SERVER_TARGETS)
	rm -f $(PRISKV_SERVER_OBJS) $(PRISKV_SERVER_DEPS) $(COMMON_LIB_OBJS) $(COMMON_LIB_DEPS)
	rm -rf $(LIBJSONC_OUTPUT)
	find $(LIBEVHTP_OUTPUT) -type f -not -name '.gitkeep' -exec rm -rf {} \; || true
	rm -f $(MAN_DIR)/priskv-server.1

format:
	$(FMT) -i *.c *.h
	$(FMT) -i backend/*.c backend/*.h
	$(FMT) -i transport/*.c transport/*.h
	make -C test format
