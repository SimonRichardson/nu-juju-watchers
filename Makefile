PROJECT_PATH=$(GOPATH)/src/github.com/SimonRichardson/nu-juju-watchers

DQLITE_S3_BUCKET=s3://dqlite-static-libs
DQLITE_S3_ARCHIVE_NAME=$(shell date -u +"%Y-%m-%d")-dqlite-deps-$(shell uname -m).tar.bz2
DQLITE_S3_ARCHIVE_PATH=${DQLITE_S3_BUCKET}/${DQLITE_S3_ARCHIVE_NAME}

DQLITE_EXTRACTED_DEPS_PATH=${PROJECT_PATH}/_deps
DQLITE_EXTRACTED_DEPS_ARCHIVE_PATH=${DQLITE_EXTRACTED_DEPS_PATH}/juju-dqlite-static-lib-deps

dqlite-deps-check:
	@if [ ! -d ${DQLITE_EXTRACTED_DEPS_ARCHIVE_PATH} ]; then \
		$(MAKE) -s dqlite-deps-pull; \
	fi

dqlite-deps-pull:
	@echo "DQLITE: Cleaning up deps path"
	@mkdir -p ${DQLITE_EXTRACTED_DEPS_PATH}
	@rm -rf ${DQLITE_EXTRACTED_DEPS_ARCHIVE_PATH}
	@echo "DQLITE: Pulling latest-juju-dqlite-static-lib-deps-$(shell uname -m).tar.bz2 from s3"
	aws s3 cp s3://dqlite-static-libs/latest-juju-dqlite-static-lib-deps-$(shell uname -m).tar.bz2 - | tar xjf - -C ${DQLITE_EXTRACTED_DEPS_PATH}

cgo-go-op: dqlite-deps-check
	PATH=${PATH}:/usr/local/musl/bin \
		CC="musl-gcc" \
		CGO_CFLAGS="-I${DQLITE_EXTRACTED_DEPS_ARCHIVE_PATH}/include" \
		CGO_LDFLAGS="-L${DQLITE_EXTRACTED_DEPS_ARCHIVE_PATH} -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="(-Wl,-wrap,pthread_create)|(-Wl,-z,now)" \
		LD_LIBRARY_PATH="${DQLITE_EXTRACTED_DEPS_ARCHIVE_PATH}" \
		CGO_ENABLED=1 \
		go $o $d \
			-mod=${JUJU_GOMOD_MODE} \
			-tags "libsqlite3 ${BUILD_TAGS}" \
			${COMPILE_FLAGS} \
			-ldflags "-s -w -linkmode 'external' -extldflags '-static'" \
			-v .

cgo-go-install:
## go-install: Install Juju binaries without updating dependencies
	$(MAKE) cgo-go-op o=install d=

cgo-go-build:
## go-build: Build Juju binaries without updating dependencies
	$(MAKE) cgo-go-op o=build d="-o ./bin/nu-juju-watchers"
