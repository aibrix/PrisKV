LIB_NAME = libpriskvcluster
STATIC_LIB = $(LIB_NAME).a
STATIC_LIB_TMP = $(LIB_NAME).tmp.a
INCFLAGS = -I../../client/ -I../../include/
UCX_CFLAGS = $(shell pkg-config --cflags ucx)
UCX_LDFLAGS = $(shell pkg-config --libs ucx)
LIBS = -lrdmacm -libverbs -lpthread -lhiredis -lncurses -luuid -ldl $(UCX_LDFLAGS)
CFLAGS = -fPIC -Wall -g -O0 $(INCFLAGS) -D_GNU_SOURCE -Wshadow -Wformat=2 -Wwrite-strings -fstack-protector-strong -Wnull-dereference -Wunreachable-code $(UCX_CFLAGS) $(LIBS)
CC = gcc
AR = ar
FMT = clang-format-19

CUDA_HOME ?= /usr/local/cuda
CUDA_LIB ?= $(CUDA_HOME)/lib64
CUDA_INC ?= $(CUDA_HOME)/include
CUDARTLIB  ?= cudart_static
CUDA_LDFLAGS = -I$(CUDA_INC) -L$(CUDA_LIB) -l$(CUDARTLIB) -lcuda -ldl -lrt

__OS_TYPE = $(shell sh ../../scripts/utils/get_os_type.sh)
ifeq ($(__OS_TYPE), unknown)
$(error "Unknown OS type")
endif

ifneq (,$(filter $(PRISKV_USE_CUDA),yes YES y Y 1))
CFLAGS += $(CUDA_LDFLAGS) -DPRISKV_USE_CUDA
endif

PRISKV_CLUSTER_TARGETS = priskv-cluster-benchmark priskv-cluster-example
PRISKV_CLUSTER_TARGETS_SRCS = $(patsubst priskv-cluster-%, %.c, $(PRISKV_CLUSTER_TARGETS))
ALL_SRCS = $(wildcard *.c)
LIB_SRCS = $(filter-out $(PRISKV_CLUSTER_TARGETS_SRCS), $(wildcard *.c))
LIB_OBJS := $(LIB_SRCS:%.c=%.o)
DEPS := $(ALL_SRCS:%.c=%.d)

COMMON_LIB_DIR = ../../lib
COMMON_LIB_SRCS = $(foreach dir,$(COMMON_LIB_DIR),$(wildcard $(dir)/*.c))
COMMON_LIB_OBJS := $(COMMON_LIB_SRCS:%.c=%.o)
COMMON_LIB_DEPS := $(COMMON_LIB_OBJS:%.o=%.d)

PRISKV_STATIC_LIB = ../../client/libpriskv.a

ifneq ($(LLVM),)
CC = clang
AR = llvm-ar-18
else
CFLAGS += -Wduplicated-branches -Wrestrict
endif

.PHONY: all rebuild clean format

all: $(PRISKV_CLUSTER_TARGETS)

%.d: %.c
	@rm -f $@; \
	$(CC) -MM $< $(INCFLAGS) >$@.tmp; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.tmp > $@; \
	rm -f $@.tmp

LIBJSONC_DIR = ../../thirdparty/json-c
include ../../lib/libjsonc.mk

ifeq ($(filter clean format, $(MAKECMDGOALS)),)
include $(DEPS)
include $(COMMON_LIB_DEPS)
endif

$(STATIC_LIB_TMP): $(DEPS_STATIC_LIBS) $(LIB_OBJS) $(COMMON_LIB_OBJS)
	$(AR) rcs $@ $^

$(STATIC_LIB): $(STATIC_LIB_TMP) $(PRISKV_STATIC_LIB) $(DEPS_STATIC_LIBS)
	$(AR) -M < ./libpriskvcluster.mri

$(PRISKV_CLUSTER_TARGETS): priskv-cluster-%: %.c $(STATIC_LIB)
	$(CC) $< -o $@ $(STATIC_LIB) $(CFLAGS)

rebuild: clean
	make all

clean:
	rm -f $(LIB_OBJS) $(DEPS) $(STATIC_LIB) $(STATIC_LIB_TMP) $(COMMON_LIB_OBJS) $(COMMON_LIB_DEPS) $(PRISKV_CLUSTER_TARGETS)

format:
	$(FMT) -i *.c *.h
