ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

LOCAL_BUILD_TARGET ?= build-debug
PREFIX := $(strip $(subst ",,$(PREFIX)))
HOSTNAME := $(shell hostname 2> /dev/null || hostnamectl hostname 2> /dev/null)
$(if $(HOSTNAME),,$(error Failed to determine hostname: both 'hostname' and 'hostnamectl' failed))

# Read the latest migration from the sibling db package. The path is relative
# to this package, not to the repository root, so it resolves both here and in
# the published repository, where this package sits at packages/api.
expectedMigration := $(shell ls ../db/migrations | sed 's/_.*//' | sort | tail -n 1)

ifeq ($(PROVIDER),aws)
	REGISTRY_PREFIX := $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(PREFIX)core
else
	REGISTRY_PREFIX := $(GCP_REGION)-docker.pkg.dev/$(GCP_PROJECT_ID)/$(PREFIX)core
endif

# E2B-hosted registry repo holding released, prebuilt api AND db-migrator
# images — they release as one unit from this package's docker-bake.hcl.
# Published to by the release-please workflow on tagging a release.
E2B_ARTIFACTS_REGISTRY ?= us-docker.pkg.dev/e2b-artifacts/api

# Architecture for the image lanes. Defaults to amd64; override for arm64:
#   BUILD_ARCH=arm64 make build-and-upload
# or for both at once (from source only):
#   BUILD_PLATFORM=linux/amd64,linux/arm64 make build-and-upload
BUILD_ARCH ?= amd64
BUILD_PLATFORM ?= linux/$(BUILD_ARCH)

# Optional version "hook" for the released-image flow.
# When empty (default), build-and-upload bakes both images from source and
# pushes to the client's own core repo (the existing flow, unchanged).
# When set (e.g. API_VERSION=v0.1.0), skip building: pull the released api
# and db-migrator images from $(E2B_ARTIFACTS_REGISTRY) and retag/push them
# into the client's core repo. One API_VERSION covers BOTH images on purpose:
# the api bakes in the migration timestamp its startup guard expects and the
# migrator bakes in the migrations, so a shared version makes schema skew
# between them impossible.
#
# Set it either on the command line (make ... API_VERSION=v0.1.0) or in the
# active .env.${ENV} file, which is included above. Quotes are stripped so
# both `=v0.1.0` and `="v0.1.0"` work; a command-line value always wins over
# the env file.
API_VERSION := $(strip $(subst ",,$(API_VERSION)))

VERSION_LDFLAGS :=
ifneq ($(strip $(VERSION)),)
VERSION_LDFLAGS := -X=main.serviceVersion=$(VERSION)
endif

.PHONY: generate
generate:
	go generate ./...

.PHONY: build
build:
	# Allow for passing commit sha directly for docker builds
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	$(eval EXPECTED_MIGRATION_TIMESTAMP ?= $(expectedMigration))
	CGO_ENABLED=0 go build -o bin/api -ldflags "-X=main.commitSHA=$(COMMIT_SHA) -X=main.expectedMigrationTimestamp=$(EXPECTED_MIGRATION_TIMESTAMP) $(VERSION_LDFLAGS)" .

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
	$(eval EXPECTED_MIGRATION_TIMESTAMP ?= $(expectedMigration))
	CGO_ENABLED=1 go build $(RACE_FLAG) $(DEBUG_GCFLAGS) -o bin/api -ldflags "-X=main.commitSHA=$(COMMIT_SHA) -X=main.expectedMigrationTimestamp=$(EXPECTED_MIGRATION_TIMESTAMP) $(VERSION_LDFLAGS)" .

.PHONY: run
run:
	make build-debug
	POSTGRES_CONNECTION_STRING=$(POSTGRES_CONNECTION_STRING) \
	AUTH_PROVIDER_CONFIG='{"jwt":[]}' \
	GOTRACEBACK=crash \
	GODEBUG=madvdontneed=1 \
	SANDBOX_ACCESS_TOKEN_HASH_SEED=$(SANDBOX_ACCESS_TOKEN_HASH_SEED) \
	CLICKHOUSE_CONNECTION_STRING=$(CLICKHOUSE_CONNECTION_STRING) \
	ENVIRONMENT=$(ENVIRONMENT) \
	ORCHESTRATOR_PORT=5008 \
	NODE_ID=integration-tests \
	LOKI_URL=unset \
	VOLUME_TOKEN_ISSUER=local.e2b.dev \
	VOLUME_TOKEN_SIGNING_METHOD=ES256 \
	VOLUME_TOKEN_SIGNING_KEY=ECDSA:LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JR0hBZ0VBTUJNR0J5cUdTTTQ5QWdFR0NDcUdTTTQ5QXdFSEJHMHdhd0lCQVFRZ2xwTHZaNnZmSGNYanBvMFYKUzVncW5JM21kdTNBSGJ0UnhmcU1CNWViT3I2aFJBTkNBQVQ3MHBiSE5NY015eFRxTVJiRjN0bXNwemZQTDIwQQpldk1lWGN5MFVDMkRiRXJ2bXBuT2hDamxFT2l4TXVDRzdUekJDdllLNUM0bGdxY0FjVnZVclBwZgotLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tCg== \
	VOLUME_TOKEN_SIGNING_KEY_NAME=local-dev-2026-03-03 \
	./bin/api --port 3000

define setup_local_env
	$(eval include .env.local)
	$(eval export $(shell sed 's/=.*//' .env.local))
endef

.PHONY: run-local
run-local: $(LOCAL_BUILD_TARGET)
	$(call setup_local_env)
	NODE_ID=$(HOSTNAME) ./bin/api --port 3000

# Run the API using air
.PHONY: dev
dev:
	go tool air

# You run the parametrized command like this:
# make metric=heap interval=90 profiler
.PHONY: profiler
profiler:
	go tool pprof -http :9991 http://localhost:6060/debug/pprof/$(metric)?seconds=$(interval)\&timeout=120

.PHONY: build-and-upload
build-and-upload:
ifeq ($(strip $(API_VERSION)),)
	# Existing flow: bake both images from source and push to the client's
	# own core repo.
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	$(eval EXPECTED_MIGRATION_TIMESTAMP := $(expectedMigration))
	cd .. && REGISTRY_PREFIX=$(REGISTRY_PREFIX) \
		COMMIT_SHA=$(COMMIT_SHA) \
		EXPECTED_MIGRATION_TIMESTAMP=$(EXPECTED_MIGRATION_TIMESTAMP) \
		PLATFORMS=$(BUILD_PLATFORM) \
		docker buildx bake -f api/docker-bake.hcl --push
else
	# Released-image flow: pull the prebuilt, versioned api + db-migrator
	# images from the E2B artifacts registry and retag/push them into the
	# client's own core repo.
	@echo "Using released api + db-migrator $(API_VERSION) from $(E2B_ARTIFACTS_REGISTRY)"
	docker pull --platform $(BUILD_PLATFORM) $(E2B_ARTIFACTS_REGISTRY)/api:$(API_VERSION)
	docker pull --platform $(BUILD_PLATFORM) $(E2B_ARTIFACTS_REGISTRY)/db-migrator:$(API_VERSION)
	docker tag $(E2B_ARTIFACTS_REGISTRY)/api:$(API_VERSION) $(REGISTRY_PREFIX)/api:latest
	docker tag $(E2B_ARTIFACTS_REGISTRY)/api:$(API_VERSION) $(REGISTRY_PREFIX)/api:$(API_VERSION)
	docker tag $(E2B_ARTIFACTS_REGISTRY)/db-migrator:$(API_VERSION) $(REGISTRY_PREFIX)/db-migrator:latest
	docker tag $(E2B_ARTIFACTS_REGISTRY)/db-migrator:$(API_VERSION) $(REGISTRY_PREFIX)/db-migrator:$(API_VERSION)
	docker push $(REGISTRY_PREFIX)/api:latest
	docker push $(REGISTRY_PREFIX)/api:$(API_VERSION)
	docker push $(REGISTRY_PREFIX)/db-migrator:latest
	docker push $(REGISTRY_PREFIX)/db-migrator:$(API_VERSION)
endif

.PHONY: test
test:
	go test -race -v ./...

.PHONY: lint
lint:
	golangci-lint run --fix ./...

.PHONY: generate-key
generate-local-key:
	# Generate a new ECDSA key pair (P-256)
	openssl ecparam -name prime256v1 -genkey -noout -out private.pem
	openssl ec -in private.pem -pubout -out public.pem
	# Update .env.local with the new private key and name stamped with today's date
	KEY=$$(base64 -w0 private.pem); \
	NAME=local-dev-$$(date +%F); \
	if grep -q '^VOLUME_TOKEN_SIGNING_KEY=' .env.local; then \
		sed -i "s|^VOLUME_TOKEN_SIGNING_KEY=.*|VOLUME_TOKEN_SIGNING_KEY=ECDSA:$${KEY}|" .env.local; \
	else \
		echo "VOLUME_TOKEN_SIGNING_KEY=ECDSA:$${KEY}" >> .env.local; \
	fi; \
	if grep -q '^VOLUME_TOKEN_SIGNING_KEY_NAME=' .env.local; then \
		sed -i "s|^VOLUME_TOKEN_SIGNING_KEY_NAME=.*|VOLUME_TOKEN_SIGNING_KEY_NAME=$${NAME}|" .env.local; \
	else \
		echo "VOLUME_TOKEN_SIGNING_KEY_NAME=$${NAME}" >> .env.local; \
	fi

	echo "Use the following base64 encoded private key to validate the token's signature:"
	echo "local-dev:ECDSA:$$(base64 -w0 public.pem)"
