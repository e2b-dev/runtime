ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

LOCAL_BUILD_TARGET ?= build-debug
PREFIX := $(strip $(subst ",,$(PREFIX)))
HOSTNAME := $(shell hostname 2> /dev/null || hostnamectl hostname 2> /dev/null)
$(if $(HOSTNAME),,$(error Failed to determine hostname: both 'hostname' and 'hostnamectl' failed))

# Architecture for builds. Defaults to amd64; override for ARM64 builds.
# (e.g., BUILD_ARCH=arm64 make build-and-upload)
BUILD_ARCH ?= amd64
# Docker platform string. Override for multi-arch builds:
#   BUILD_PLATFORM=linux/amd64,linux/arm64 make build-and-upload
BUILD_PLATFORM ?= linux/$(BUILD_ARCH)

ifeq ($(PROVIDER),aws)
	IMAGE_REGISTRY := $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(PREFIX)core/client-proxy
else ifeq ($(PROVIDER),azure)
	IMAGE_REGISTRY := $(AZURE_REGISTRY_NAME).azurecr.io/$(PREFIX)core/client-proxy
else
	IMAGE_REGISTRY := $(GCP_REGION)-docker.pkg.dev/$(GCP_PROJECT_ID)/$(PREFIX)core/client-proxy
endif

# E2B-hosted registry that holds released, prebuilt client-proxy images.
# Published to by the release-please workflow on tagging a release.
E2B_ARTIFACTS_REGISTRY ?= us-docker.pkg.dev/e2b-artifacts/client-proxy/client-proxy

# Optional version "hook" for the released-image flow.
# When empty (default), build-and-upload builds from source and pushes to the
# client's own core repo (the existing flow, unchanged).
# When set (e.g. CLIENT_PROXY_VERSION=v0.1.0), skip building: pull the released
# image from $(E2B_ARTIFACTS_REGISTRY) and retag/push it into the client's core
# repo, so the Terraform `client-proxy:latest` lookup keeps working.
#
# Set it either on the command line (make ... CLIENT_PROXY_VERSION=v0.1.0) or in
# the active .env.${ENV} file (CLIENT_PROXY_VERSION=v0.1.0), which is included
# above. Quotes are stripped so both `=v0.1.0` and `="v0.1.0"` work; a
# command-line value always wins over the env file.
CLIENT_PROXY_VERSION := $(strip $(subst ",,$(CLIENT_PROXY_VERSION)))

VERSION_LDFLAGS :=
ifneq ($(strip $(VERSION)),)
VERSION_LDFLAGS := -X=main.version=$(VERSION)
endif

.PHONY: build
build:
	# Allow for passing commit sha directly for docker builds
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	CGO_ENABLED=0 GOOS=linux GOARCH=$(BUILD_ARCH) go build -o bin/client-proxy -ldflags "-X=main.commitSHA=$(COMMIT_SHA) $(VERSION_LDFLAGS)" .

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
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	CGO_ENABLED=1 go build $(RACE_FLAG) $(DEBUG_GCFLAGS) -o bin/client-proxy -ldflags "-X=main.commitSHA=$(COMMIT_SHA) $(VERSION_LDFLAGS)" .

.PHONY: build-and-upload
build-and-upload:
ifeq ($(strip $(CLIENT_PROXY_VERSION)),)
	# Existing flow: build from source and push to the client's own core repo.
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	@docker buildx build --platform $(BUILD_PLATFORM) --tag $(IMAGE_REGISTRY) --push --build-arg COMMIT_SHA="$(COMMIT_SHA)" -f ./Dockerfile ..
else
	# Released-image flow: pull the prebuilt, versioned image from the E2B
	# artifacts registry and retag/push it into the client's own core repo.
	@echo "Using released client-proxy $(CLIENT_PROXY_VERSION) from $(E2B_ARTIFACTS_REGISTRY)"
	docker pull --platform $(BUILD_PLATFORM) $(E2B_ARTIFACTS_REGISTRY):$(CLIENT_PROXY_VERSION)
	docker tag $(E2B_ARTIFACTS_REGISTRY):$(CLIENT_PROXY_VERSION) $(IMAGE_REGISTRY):latest
	docker tag $(E2B_ARTIFACTS_REGISTRY):$(CLIENT_PROXY_VERSION) $(IMAGE_REGISTRY):$(CLIENT_PROXY_VERSION)
	docker push $(IMAGE_REGISTRY):latest
	docker push $(IMAGE_REGISTRY):$(CLIENT_PROXY_VERSION)
endif

.PHONY: run
run:
	make build-debug
	NODE_ID=integration-tests \
	NODE_IP=127.0.0.1 \
	./bin/client-proxy

define setup_local_env
	$(eval include .env.local)
	$(eval export $(shell sed 's/=.*//' .env.local))
endef

.PHONY: run-local
run-local: $(LOCAL_BUILD_TARGET)
	$(call setup_local_env)
	NODE_ID=$(HOSTNAME) ./bin/client-proxy

.PHONY: generate
generate:
	go generate ./...

.PHONY: test
test:
	go test -race -v ./...
