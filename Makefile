FMT = clang-format-19
DISTOS := $(shell sh scripts/utils/get_os_dist.sh)

PRISKV_VERSION=$(shell git rev-parse --short HEAD)$(shell git diff HEAD --ignore-submodules=all --quiet || echo -dirty)
ifneq ($(PRISKV_PKG_VERSION),)
	PRISKV_VERSION=$(PRISKV_PKG_VERSION)
endif

__OS_TYPE = $(shell sh scripts/utils/get_os_type.sh)
ifeq ($(__OS_TYPE), unknown)
$(error "Unknown OS type")
endif

__OS_NAME = $(shell sh scripts/utils/get_os_name.sh)
ifeq ($(__OS_NAME), unknown)
$(error "Unknown OS name")
endif

.PHONY: all server client cluster test rebuild clean format pyclient pyclusterclient

all: server client cluster pyclient pyclusterclient test

version:
	@echo "PrisKV Version: $(PRISKV_VERSION)"
	sed 's/__PRISKV_VERSION__ "unknown"/__PRISKV_VERSION__ "$(PRISKV_VERSION)"/g' include/priskv-version.h.in > include/priskv-version.h

server: version client
	make -C server all

client: version
	make -C client all

cluster: client
	make -C cluster all

pyclient: client
	make -C pypriskv all

pyclusterclient: client
	make -C pypriskvcluster all

test: version
	make -C server test
	make -C lib test

rebuild: clean
	make all

clean: version
	make -C server clean
	make -C client clean
	make -C cluster clean
	make -C lib clean
	make -C pypriskv clean
	make -C pypriskvcluster clean
	rm -f include/priskv-version.h

yapf_check:
	@if ! command -v yapf >/dev/null 2>&1; then \
        echo "yapf is not installed. Please install it first."; \
        exit 1; \
    fi

format: version yapf_check
	make -C server format
	make -C client format
	make -C cluster format
	make -C lib format
	make -C include format
	find . -path './thirdparty' -prune -o -name "*.py" -exec yapf --in-place {} \;

pkg: export PRISKV_RELEASE=1
pkg: version
	mkdir -p output/$(__OS_NAME)
ifeq ($(__OS_TYPE), redhat)
	bash scripts/build/build_rpm.sh output/$(__OS_NAME)
else
	sed "s/DISTOS/$(DISTOS)/g" debian/changelog.tpl > debian/changelog
	dpkg-buildpackage -b -uc -us
	mv ../priskv-* output/$(__OS_NAME)
	mv ../priskv_* output/$(__OS_NAME)
endif

pyclient-pkg: pyclient
	mkdir -p output/$(__OS_NAME)

	mv pypriskv/dist/priskv-*.whl output/$(__OS_NAME)

prisclient-pkg: pyclusterclient
	mkdir -p output/$(__OS_NAME)

	mv pypriskvcluster/dist/pris-*.whl output/$(__OS_NAME)

pkg-clean:
ifeq ($(__OS_TYPE), redhat)
	$(error "Redhat is not supported yet")
else
	debian/rules clean
endif
	rm -rf output
	rm -rf debian/changelog

install-%: version
	make -C $* install PRISKV_DESTDIR=$(PRISKV_DESTDIR)

# for example: make pkg-ubuntu2004, and you will get a deb package in ./output/ubuntu20.04/
pkg-%:
	@set -e; \
	BUILD_ENV=$*; \
	IMAGE_NAME="priskv-pkg:$${BUILD_ENV}"; \
	CONTAINER_NAME="priskv-pkg-$${BUILD_ENV}-$(shell date +%s)"; \
	docker build . -t $${IMAGE_NAME} --network=host -f ./docker/Dockerfile_$${BUILD_ENV}; \
	docker create --name $${CONTAINER_NAME} $${IMAGE_NAME}; \
	docker cp $${CONTAINER_NAME}:/root/priskv/output .; \
	docker rm -f $${CONTAINER_NAME}; \
	echo "Succeed to build package to ./output/"
