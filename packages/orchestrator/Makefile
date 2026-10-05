ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

AWS_BUCKET_PREFIX ?= $(PREFIX)$(AWS_ACCOUNT_ID)-
GCP_BUCKET_PREFIX ?= $(GCP_PROJECT_ID)-
CLEAN_NFS_CACHE_IMAGE_REGISTRY := $(GCP_REGION)-docker.pkg.dev/$(GCP_PROJECT_ID)/$(PREFIX)core/clean-nfs-cache

# Public bucket holding released, prebuilt orchestrator-package binaries
# (objects are <version>/orchestrator and <version>/clean-nfs-cache; there is
# no separate template-manager object — it is the orchestrator binary
# uploaded under a second name, matching upload/template-manager below).
# Published to by the release-please workflow on tagging a release;
# public-read, so downloads need no credentials.
E2B_ARTIFACT_BINARIES_URL ?= https://storage.googleapis.com/e2b-artifact-binaries/orchestrator

# Optional version "hook" for the released-artifact flow.
# When empty (default), the build-and-upload/* targets build from source and
# upload the binaries to the client's fc-env-pipeline bucket (the existing
# flow, unchanged). When set (e.g. ORCHESTRATOR_VERSION=v0.1.0), skip
# building: download the released binaries and upload those instead. The env
# pipeline keeps fetching from the same bucket objects either way. One
# version covers orchestrator, template-manager, and clean-nfs-cache — they
# release together.
#
# Set it either on the command line (make ... ORCHESTRATOR_VERSION=v0.1.0) or
# in the active .env.${ENV} file, which is included above. Quotes are
# stripped so both `=v0.1.0` and `="v0.1.0"` work; a command-line value
# always wins over the env file.
ORCHESTRATOR_VERSION := $(strip $(subst ",,$(ORCHESTRATOR_VERSION)))
ifneq ($(ORCHESTRATOR_VERSION),)
ifeq ($(filter v%,$(ORCHESTRATOR_VERSION)),)
$(error ORCHESTRATOR_VERSION must be a v-prefixed version (e.g. v0.15.0), not a commit SHA)
endif
endif

HOSTNAME := $(shell hostname 2> /dev/null || hostnamectl hostname 2> /dev/null)
$(if $(HOSTNAME),,$(error Failed to determine hostname: both 'hostname' and 'hostnamectl' failed))

# Architecture for builds. Defaults to amd64; override for ARM64 builds.
# (e.g., BUILD_ARCH=arm64 make build-local)
BUILD_ARCH ?= amd64
# Docker platform string. Override for cross-platform builds:
#   BUILD_PLATFORM=linux/arm64 make build
BUILD_PLATFORM ?= linux/$(BUILD_ARCH)

# Busybox version — single source of truth for both Docker and local builds.
BUSYBOX_VERSION ?= 1.36.1
BUSYBOX_LOCAL := .busybox/$(BUSYBOX_VERSION)/$(BUILD_ARCH)/busybox

VERSION_LDFLAGS :=
ifneq ($(strip $(VERSION)),)
VERSION_LDFLAGS := -X=github.com/e2b-dev/infra/packages/orchestrator/pkg/version.Version=$(VERSION)
endif
BUILD_TAGS :=
STATIC_LDFLAGS :=
ifeq ($(STATIC),1)
BUILD_TAGS := -tags netgo,osusergo
STATIC_LDFLAGS := -linkmode external -extldflags -static
endif

.PHONY: init
init:
	brew install protobuf

.PHONY: generate
generate:
	go generate ./...

.PHONY: build
build:
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	@docker build --platform $(BUILD_PLATFORM) --target artifacts --output=bin --build-arg COMMIT_SHA="$(COMMIT_SHA)" --build-arg VERSION="$(VERSION)" -f ./Dockerfile ..

.PHONY: build-image
build-image:
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	docker build --platform $(BUILD_PLATFORM) --build-arg COMMIT_SHA="$(COMMIT_SHA)" --build-arg VERSION="$(VERSION)" -t orchestrator:local -f ./Dockerfile ..

# Download busybox from GCS public builds bucket.
# Skips if binary exists and version/arch match the stamp file.
.PHONY: fetch-busybox
fetch-busybox:
	@./scripts/fetch-busybox.sh "$(BUSYBOX_VERSION)" "$(BUILD_ARCH)" "$(BUSYBOX_LOCAL)"

.PHONY: build-local
build-local:
	# Allow for passing commit sha directly for docker builds
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	CGO_ENABLED=1 GOOS=linux GOARCH=$(BUILD_ARCH) go build $(BUILD_TAGS) -o bin/orchestrator -ldflags "-X=main.commitSHA=$(COMMIT_SHA) $(VERSION_LDFLAGS) $(STATIC_LDFLAGS)" .
	CGO_ENABLED=1 GOOS=linux GOARCH=$(BUILD_ARCH) go build $(BUILD_TAGS) -o bin/clean-nfs-cache -ldflags "-X=main.commitSHA=$(COMMIT_SHA) $(VERSION_LDFLAGS) $(STATIC_LDFLAGS)" ./cmd/clean-nfs-cache

# Compiler flags for debug builds. The default disables optimizations and
# inlining so binaries are debugger-friendly. CI overrides this with an empty
# value: race-instrumented but optimized binaries compile faster and speed up
# every integration test at runtime.
DEBUG_GCFLAGS ?= -gcflags=all="-N -l"
# Race instrumentation of the debug build, overridable (RACE_FLAG="") so CI
# can run some suite configurations against uninstrumented binaries.
RACE_FLAG ?= -race

.PHONY: build-debug
build-debug:
	CGO_ENABLED=1 GOOS=linux GOARCH=$(BUILD_ARCH) go build $(RACE_FLAG) $(DEBUG_GCFLAGS) -o bin/orchestrator .

# Builds the dummy orchestrator. It only implements the orchestrator.proto
# SandboxService surface against an in-memory store, and is meant for local
# API development and lightweight tests where the full Linux orchestrator
# (firecracker, NBD, NFS, cgroups) is not needed.
.PHONY: build-dummy
build-dummy:
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	CGO_ENABLED=0 go build -o bin/dummy-orchestrator -ldflags "-X=main.commitSHA=$(COMMIT_SHA)" ./cmd/dummy-orchestrator

.PHONY: run-dummy
run-dummy: build-dummy
	NODE_ID=$(HOSTNAME) GRPC_PORT=5008 HEALTH_PORT=5018 ./bin/dummy-orchestrator

.PHONY: run-debug
run-debug:
	make build-debug
	sudo -E GOTRACEBACK=crash \
	GODEBUG=madvdontneed=1 \
	NODE_ID="testclient" \
	TEMPLATE_BUCKET_NAME=$(TEMPLATE_BUCKET_NAME) \
	ENVD_TIMEOUT=$(ENVD_TIMEOUT) \
	ORCHESTRATOR_SERVICES=$(ORCHESTRATOR_SERVICES) \
	GCP_DOCKER_REPOSITORY_NAME=$(GCP_DOCKER_REPOSITORY_NAME) \
	GOOGLE_SERVICE_ACCOUNT_BASE64=$(GOOGLE_SERVICE_ACCOUNT_BASE64) \
	OTEL_COLLECTOR_GRPC_ENDPOINT=$(OTEL_COLLECTOR_GRPC_ENDPOINT) \
	MAX_PARALLEL_MEMFILE_SNAPSHOTTING=$(MAX_PARALLEL_MEMFILE_SNAPSHOTTING) \
	./bin/orchestrator

define setup_local_env
	$(eval include .env.local)
	$(eval export $(shell sed 's/=.*//' .env.local))
endef

.PHONY: run-local
run-local:
	$(call setup_local_env)
	mkdir -p ./.data/test-volume
	NODE_ID=$(HOSTNAME) ./bin/orchestrator

.PHONY: upload/clean-nfs-cache
upload/clean-nfs-cache:
	chmod +x ./bin/clean-nfs-cache
ifeq ($(PROVIDER),aws)
	aws s3 cp ./bin/clean-nfs-cache "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/clean-nfs-cache" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
ifneq ($(strip $(ORCHESTRATOR_VERSION)),)
	aws s3 cp ./bin/clean-nfs-cache "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/clean-nfs-cache.$(ORCHESTRATOR_VERSION)" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
endif
else
	gcloud storage cp --cache-control="no-cache, max-age=0" ./bin/clean-nfs-cache "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/clean-nfs-cache"
ifneq ($(strip $(ORCHESTRATOR_VERSION)),)
	gcloud storage cp --cache-control="no-cache, max-age=0" ./bin/clean-nfs-cache "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/clean-nfs-cache.$(ORCHESTRATOR_VERSION)"
endif
endif

.PHONY: upload/orchestrator
upload/orchestrator:
	chmod +x ./bin/orchestrator
ifeq ($(PROVIDER),aws)
	aws s3 cp ./bin/orchestrator "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/orchestrator" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
ifneq ($(strip $(ORCHESTRATOR_VERSION)),)
	aws s3 cp ./bin/orchestrator "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/orchestrator.$(ORCHESTRATOR_VERSION)" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
endif
else
	gcloud storage cp --cache-control="no-cache, max-age=0" ./bin/orchestrator "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/orchestrator"
ifneq ($(strip $(ORCHESTRATOR_VERSION)),)
	gcloud storage cp --cache-control="no-cache, max-age=0" ./bin/orchestrator "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/orchestrator.$(ORCHESTRATOR_VERSION)"
endif
endif

.PHONY: upload/template-manager
upload/template-manager:
	chmod +x ./bin/orchestrator
ifeq ($(PROVIDER),aws)
	aws s3 cp ./bin/orchestrator "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/template-manager" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
ifneq ($(strip $(ORCHESTRATOR_VERSION)),)
	aws s3 cp ./bin/orchestrator "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/template-manager.$(ORCHESTRATOR_VERSION)" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
endif
else
	gcloud storage cp --cache-control="no-cache, max-age=0" ./bin/orchestrator "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/template-manager"
ifneq ($(strip $(ORCHESTRATOR_VERSION)),)
	gcloud storage cp --cache-control="no-cache, max-age=0" ./bin/orchestrator "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/template-manager.$(ORCHESTRATOR_VERSION)"
endif
endif

# Released-artifact flow: download the prebuilt, versioned binaries from the
# public E2B artifact-binaries bucket.
.PHONY: pull-released
pull-released:
	@echo "Using released orchestrator $(ORCHESTRATOR_VERSION) from $(E2B_ARTIFACT_BINARIES_URL)"
	mkdir -p bin
	curl -fL -o bin/orchestrator "$(E2B_ARTIFACT_BINARIES_URL)/$(ORCHESTRATOR_VERSION)/orchestrator"
	curl -fL -o bin/clean-nfs-cache "$(E2B_ARTIFACT_BINARIES_URL)/$(ORCHESTRATOR_VERSION)/clean-nfs-cache"

# The binaries come from source (default) or from released objects when
# ORCHESTRATOR_VERSION is set. Either way they land in ./bin and the
# upload/* targets are identical.
ifeq ($(strip $(ORCHESTRATOR_VERSION)),)
BIN_SOURCE := build
else
BIN_SOURCE := pull-released
endif

.PHONY: build-and-upload/clean-nfs-cache
build-and-upload/clean-nfs-cache: $(BIN_SOURCE) upload/clean-nfs-cache

.PHONY: build-and-upload/clean-nfs-cache-image
build-and-upload/clean-nfs-cache-image: $(BIN_SOURCE)
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	@docker buildx build --platform $(BUILD_PLATFORM) --tag $(CLEAN_NFS_CACHE_IMAGE_REGISTRY) --push --build-arg COMMIT_SHA="$(COMMIT_SHA)" -f ./Dockerfile.clean-nfs-cache .

.PHONY: build-and-upload/orchestrator
build-and-upload/orchestrator: $(BIN_SOURCE) upload/orchestrator

.PHONY: build-and-upload/template-manager
build-and-upload/template-manager: $(BIN_SOURCE) upload/template-manager


.PHONY: test
test:
	go test -race -v ./...

.PHONY: test-image test-orchestrator-image
test-image:
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	@set -eu; \
	image="orchestrator:test-$(COMMIT_SHA)-$$$$"; \
	trap 'docker image rm --force "$$image" >/dev/null 2>&1 || true' EXIT; \
	docker build --load --platform $(BUILD_PLATFORM) --tag "$$image" --build-arg COMMIT_SHA="$(COMMIT_SHA)" --build-arg VERSION="$(VERSION)" -f ./Dockerfile ..; \
		docker run --rm --entrypoint /bin/bash "$$image" -eu -c '\
		test -s /etc/ssl/certs/ca-certificates.crt && \
		ldd_output="$$(ldd /usr/local/bin/orchestrator 2>&1)" && \
		printf "%s\n" "$$ldd_output" | grep -q "libc.so.6 =>" && \
		! printf "%s\n" "$$ldd_output" | grep -Eq "not found|version .* not found" && \
		command -v bash >/dev/null && \
		command -v nsenter >/dev/null && \
		command -v systemd-run >/dev/null && \
		command -v systemctl >/dev/null && \
		command -v ip >/dev/null && \
		command -v iptables >/dev/null && \
		command -v rsync >/dev/null && \
		command -v mkfs.ext4 >/dev/null && \
		test -x /usr/local/bin/orchestrator'

test-orchestrator-image: test-image

.PHONY: test-clean-nfs-cache-image
test-clean-nfs-cache-image: build
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	@set -eu; \
	image="clean-nfs-cache:test-$(COMMIT_SHA)-$$$$"; \
	trap 'docker image rm --force "$$image" >/dev/null 2>&1 || true' EXIT; \
	docker build --load --platform $(BUILD_PLATFORM) --tag "$$image" --build-arg COMMIT_SHA="$(COMMIT_SHA)" -f ./Dockerfile.clean-nfs-cache .; \
	docker run --rm "$$image" --help >/dev/null; \
	docker run --rm --entrypoint /bin/sh "$$image" -eu -c 'test -s /etc/ssl/certs/ca-certificates.crt'

.PHONY: test-docker
test-docker:
	@echo "Running orchestrator tests in Docker ($(BUILD_PLATFORM))..."
	@rm -rf .shared/
	@cp -r ../shared .shared/
	@rm -rf .clickhouse/
	@cp -r ../clickhouse .clickhouse/
	@docker build --platform $(BUILD_PLATFORM) -f test.Dockerfile --no-cache-filter runner --progress=plain -t orchestrator-test .
	@rm -rf .shared/
	@rm -rf .clickhouse/
	@echo "Done"

.PHONY: build-template
# Disk size (MB) of the base template rootfs. Matches create-build's own
# default; callers (e.g. integration-test CI) can override to leave headroom
# for disk-heavy build steps after the build-reserved-disk-space-mb reservation.
DISK_SIZE_MB ?= 1024
# Guest memory of the built template. 256 MiB runs the kernel, envd and
# light commands comfortably, and a small memfile keeps every pause, resume
# and compression proportionally cheap; pass MEMORY_MB for a bigger guest.
MEMORY_MB ?= 256
build-template: fetch-busybox
	TEMPLATE_BUCKET_NAME=$(TEMPLATE_BUCKET_NAME) \
	GOOGLE_SERVICE_ACCOUNT_BASE64=$(GOOGLE_SERVICE_ACCOUNT_BASE64) \
	DOCKER_AUTH_BASE64=$(DOCKER_AUTH_BASE64) \
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) \
	GCP_DOCKER_REPOSITORY_NAME=$(GCP_DOCKER_REPOSITORY_NAME) \
	GCP_REGION=$(GCP_REGION) \
	COMPRESS_ENABLED=$(COMPRESS_ENABLED) \
	COMPRESS_TYPE=$(COMPRESS_TYPE) \
	COMPRESS_LEVEL=$(COMPRESS_LEVEL) \
	COMPRESS_FRAME_ENCODE_WORKERS=$(COMPRESS_FRAME_ENCODE_WORKERS) \
	ENVIRONMENT=local \
	go run cmd/create-build/main.go \
	-template $(TEMPLATE_ID) \
	-to-build $(BUILD_ID) \
	-kernel $(KERNEL_VERSION) \
	-firecracker $(FIRECRACKER_VERSION) \
	-disk $(DISK_SIZE_MB) \
	-memory $(MEMORY_MB)

.PHONY: lint
lint:
	golangci-lint run --fix ./...
