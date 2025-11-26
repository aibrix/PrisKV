LIB_NAME = libpriskv
HEADERS = priskv.h
MANPAGE = ../man/libpriskv.7
STATIC_LIB = $(LIB_NAME).a
INCFLAGS = -I../include
CFLAGS = -fPIC -Wall -g -O0 $(INCFLAGS) -D_GNU_SOURCE -Wshadow -Wformat=2 -Wwrite-strings -fstack-protector-strong -Wnull-dereference -Wunreachable-code -lncurses -lpthread -luuid
CC = gcc
AR = ar
FMT = clang-format-19
__OS_TYPE = $(shell sh ../scripts/utils/get_os_type.sh)

PRISKV_BINPATH = usr/bin
PRISKV_INCLUDEPATH = usr/include/priskv
PRISKV_MANPATH = usr/share/man
ifeq ($(__OS_TYPE), redhat)
PRISKV_LIBPATH = usr/lib64
else ifeq ($(__OS_TYPE), velinux)
PRISKV_LIBPATH = usr/lib64
else ifeq ($(__OS_TYPE), debian)
PRISKV_LIBPATH = usr/lib
else
$(error "Unknown OS type")
endif

VALKEY_BENCHMARK_NAME = valkey-benchmark
VALKEY_BENCHMARK_SRC = valkey_bench.c
VALKEY_PATH = ../thirdparty/libvalkey
VALKEY_INCLUDE_PATH = $(VALKEY_PATH)/include
VALKEY_LIB_PATH = $(VALKEY_PATH)/lib
VALKEY_STATIC_LIB = $(VALKEY_LIB_PATH)/libvalkey_rdma.a $(VALKEY_LIB_PATH)/libvalkey.a
VALKEY_LDFLAGS = -I$(VALKEY_INCLUDE_PATH)

PRISKV_TARGETS_ALL = priskv-client priskv-benchmark priskv-example priskv-test_runtime
PRISKV_TARGETS = priskv-client priskv-benchmark
PRISKV_TARGETS_SRCS = $(patsubst priskv-%, %.c, $(PRISKV_TARGETS_ALL))
SRCS = $(filter-out $(PRISKV_TARGETS_SRCS) $(VALKEY_BENCHMARK_SRC), $(wildcard *.c transport/*.c))
OBJS := $(SRCS:%.c=%.o)
DEPS := $(OBJS:%.o=%.d)

COMMON_LIB_DIR = ../lib
COMMON_LIB_SRCS = $(foreach dir,$(COMMON_LIB_DIR),$(wildcard $(dir)/*.c))
COMMON_LIB_OBJS := $(COMMON_LIB_SRCS:%.c=%.o)
COMMON_LIB_DEPS := $(COMMON_LIB_OBJS:%.o=%.d)

LIBIBVERBS_VERSION = $(shell pkg-config --modversion libibverbs)
LIBIBVERBS_VERSION_MAJOR = $(shell echo ${LIBIBVERBS_VERSION} | cut -d "." -f 1)
LIBIBVERBS_VERSION_MINOR = $(shell echo ${LIBIBVERBS_VERSION} | cut -d "." -f 2)
RDMA_LDFLAGS = -lrdmacm -libverbs -DLIBIBVERBS_VERSION_MAJOR=$(LIBIBVERBS_VERSION_MAJOR) -DLIBIBVERBS_VERSION_MINOR=$(LIBIBVERBS_VERSION_MINOR)

UCX_CFLAGS = $(shell pkg-config --cflags ucx)
UCX_LDFLAGS = $(shell pkg-config --libs ucx)

CUDA_HOME ?= /usr/local/cuda
CUDA_LIB ?= $(CUDA_HOME)/lib64
CUDA_INC ?= $(CUDA_HOME)/include
CUDARTLIB  ?= cudart_static
CUDA_LDFLAGS = -I$(CUDA_INC) -L$(CUDA_LIB) -l$(CUDARTLIB) -lcuda -ldl -lrt

ifneq (,$(filter $(PRISKV_USE_CUDA),yes YES y Y 1))
PRISKV_TARGETS += priskv-example
PRISKV_TARGETS += priskv-test_runtime
CFLAGS += $(CUDA_LDFLAGS) -DPRISKV_USE_CUDA
endif

ifneq (,$(filter $(PRISKV_USE_ACL),yes YES y Y 1))
ACL_HOME ?= /usr/local/Ascend/ascend-toolkit/latest
ACL_INC ?= $(ACL_HOME)/include
ACL_LDFLAGS = -I$(ACL_INC) -L$(ACL_HOME)/lib64 -L$(ACL_HOME)/x86_64-linux/devlib/ -lascend_hal -lascendcl

CFLAGS += $(ACL_LDFLAGS) -DPRISKV_USE_ACL
endif

THIRDPARTY_SRCS = ../thirdparty/linenoise/linenoise.c
THIRDPARTY_INCFLAGS = -I../thirdparty/linenoise/

ifneq ($(LLVM),)
CC = clang
AR = llvm-ar-18
else
CFLAGS += -Wduplicated-branches -Wrestrict
endif

.PHONY: all rebuild clean format

all: $(PRISKV_TARGETS)

%.d: %.c
	@rm -f $@; \
	$(CC) -MM $< $(INCFLAGS) >$@.tmp; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.tmp > $@; \
	rm -f $@.tmp

LIBJSONC_DIR = ../thirdparty/json-c
include ../lib/libjsonc.mk

ifeq ($(filter clean format, $(MAKECMDGOALS)),)
include $(DEPS)
include $(COMMON_LIB_DEPS)
endif

$(STATIC_LIB): $(DEPS_STATIC_LIBS) $(OBJS) $(COMMON_LIB_OBJS)
	$(AR) rcs $@ $^

$(THIRDPARTY_SRCS):
	git submodule update --init --recursive ../thirdparty/linenoise

$(PRISKV_TARGETS): priskv-%: %.c $(STATIC_LIB) $(THIRDPARTY_SRCS)
	$(CC) $< $(THIRDPARTY_SRCS) -o $@ $(STATIC_LIB) $(CFLAGS) $(RDMA_LDFLAGS) $(UCX_CFLAGS) $(UCX_LDFLAGS) $(THIRDPARTY_INCFLAGS)

$(VALKEY_STATIC_LIB):
	cd $(VALKEY_PATH) && USE_RDMA=1 make

$(VALKEY_BENCHMARK_NAME): $(VALKEY_BENCHMARK_SRC) $(STATIC_LIB) $(VALKEY_STATIC_LIB)
	$(CC) $^ $(CFLAGS) $(RDMA_LDFLAGS) $(UCX_CFLAGS) $(UCX_LDFLAGS) $(CUDA_LDFLAGS) $(VALKEY_LDFLAGS) -o $@

install: $(PRISKV_TARGETS)
	install -m 755 -d $(PRISKV_DESTDIR)/$(PRISKV_BINPATH)
	install -m 755 $(PRISKV_TARGETS) $(PRISKV_DESTDIR)/$(PRISKV_BINPATH)
	install -m 755 -d $(PRISKV_DESTDIR)/$(PRISKV_INCLUDEPATH)
	install -m 755 $(HEADERS) $(PRISKV_DESTDIR)/$(PRISKV_INCLUDEPATH)
	install -m 755 -d $(PRISKV_DESTDIR)/$(PRISKV_LIBPATH)
	install -m 755 $(STATIC_LIB) $(PRISKV_DESTDIR)/$(PRISKV_LIBPATH)
	install -m 755 -d $(PRISKV_DESTDIR)/$(PRISKV_MANPATH)/man7
	install -m 755 $(MANPAGE) $(PRISKV_DESTDIR)/$(PRISKV_MANPATH)/man7

rebuild: clean
	make all

clean:
	rm -f $(OBJS) $(DEPS) $(STATIC_LIB) $(COMMON_LIB_OBJS) $(COMMON_LIB_DEPS)
	rm -f $(PRISKV_TARGETS) $(VALKEY_BENCHMARK_NAME)

format:
	$(FMT) -i *.c *.h
	$(FMT) -i transport/*.c transport/*.h
