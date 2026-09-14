ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}
PREFIX := $(strip $(subst ",,$(PREFIX)))

# Read the latest migration from the sibling db package. The path is relative
# to this package, not to the repository root, so it resolves both here and in
# the published repository, where this package sits at packages/dashboard-api.
expectedMigration := $(shell ls ../db/migrations | sed 's/_.*//' | sort | tail -n 1)

ifeq ($(PROVIDER),aws)
	IMAGE_REGISTRY := $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/$(PREFIX)core/dashboard-api
else
	IMAGE_REGISTRY := $(GCP_REGION)-docker.pkg.dev/$(GCP_PROJECT_ID)/$(PREFIX)core/dashboard-api
endif

# E2B-hosted registry that holds released, prebuilt dashboard-api images.
# Published to by the release-please workflow on tagging a release.
E2B_ARTIFACTS_REGISTRY ?= us-docker.pkg.dev/e2b-artifacts/dashboard-api/dashboard-api

# Optional version "hook" for the released-image flow.
# When empty (default), build-and-upload builds from source and pushes to the
# client's own core repo (the existing flow, unchanged).
# When set (e.g. DASHBOARD_API_VERSION=v0.1.0), skip building: pull the
# released image from $(E2B_ARTIFACTS_REGISTRY) and retag/push it into the
# client's core repo, so the `dashboard-api:latest` lookup keeps working.
#
# Set it either on the command line (make ... DASHBOARD_API_VERSION=v0.1.0)
# or in the active .env.${ENV} file, which is included above. Quotes are
# stripped so both `=v0.1.0` and `="v0.1.0"` work; a command-line value
# always wins over the env file.
#
# Keep this version in lockstep with API_VERSION. The API release's db-migrator
# carries the migrations dashboard-api expects during its startup check.
DASHBOARD_API_VERSION := $(strip $(subst ",,$(DASHBOARD_API_VERSION)))

VERSION_LDFLAGS :=
ifneq ($(strip $(VERSION)),)
VERSION_LDFLAGS := -X=main.serviceVersion=$(VERSION)
endif

HOSTNAME := $(shell hostname 2> /dev/null || hostnamectl hostname 2> /dev/null)
$(if $(HOSTNAME),,$(error Failed to determine hostname: both 'hostname' and 'hostnamectl' failed))

.PHONY: generate
generate:
	go generate ./...

.PHONY: build
build:
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	$(eval EXPECTED_MIGRATION_TIMESTAMP ?= $(expectedMigration))
	CGO_ENABLED=0 go build -o bin/dashboard-api -ldflags "-X=main.commitSHA=$(COMMIT_SHA) -X=main.expectedMigrationTimestamp=$(EXPECTED_MIGRATION_TIMESTAMP) $(VERSION_LDFLAGS)" .

.PHONY: build-debug
build-debug:
	$(eval COMMIT_SHA ?= $(shell git rev-parse --short HEAD))
	$(eval EXPECTED_MIGRATION_TIMESTAMP ?= $(expectedMigration))
	CGO_ENABLED=1 go build -race -gcflags=all="-N -l" -o bin/dashboard-api -ldflags "-X=main.commitSHA=$(COMMIT_SHA) -X=main.expectedMigrationTimestamp=$(EXPECTED_MIGRATION_TIMESTAMP) $(VERSION_LDFLAGS)" .

.PHONY: build-and-upload
build-and-upload:
ifeq ($(strip $(DASHBOARD_API_VERSION)),)
	# Existing flow: build from source and push to the client's own core repo.
	$(eval COMMIT_SHA := $(shell git rev-parse --short HEAD))
	$(eval EXPECTED_MIGRATION_TIMESTAMP := $(expectedMigration))
	@docker build --platform linux/amd64 --tag $(IMAGE_REGISTRY) --push --build-arg COMMIT_SHA="$(COMMIT_SHA)" --build-arg EXPECTED_MIGRATION_TIMESTAMP="$(EXPECTED_MIGRATION_TIMESTAMP)" -f ./Dockerfile ..
else
	# Released-image flow: pull the prebuilt, versioned image from the E2B
	# artifacts registry and retag/push it into the client's own core repo.
	@echo "Using released dashboard-api $(DASHBOARD_API_VERSION) from $(E2B_ARTIFACTS_REGISTRY)"
	docker pull --platform linux/amd64 $(E2B_ARTIFACTS_REGISTRY):$(DASHBOARD_API_VERSION)
	docker tag $(E2B_ARTIFACTS_REGISTRY):$(DASHBOARD_API_VERSION) $(IMAGE_REGISTRY):latest
	docker tag $(E2B_ARTIFACTS_REGISTRY):$(DASHBOARD_API_VERSION) $(IMAGE_REGISTRY):$(DASHBOARD_API_VERSION)
	docker push $(IMAGE_REGISTRY):latest
	docker push $(IMAGE_REGISTRY):$(DASHBOARD_API_VERSION)
endif

.PHONY: run
run:
	make build
	@./bin/dashboard-api

.PHONY: run-local
run-local:
	make build
	@NODE_ID=$(HOSTNAME) ./bin/dashboard-api

.PHONY: test
test:
	go test -race -v ./...
