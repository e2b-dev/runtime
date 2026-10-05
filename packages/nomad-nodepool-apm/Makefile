ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

AWS_BUCKET_PREFIX ?= $(PREFIX)$(AWS_ACCOUNT_ID)-
GCP_BUCKET_PREFIX ?= $(GCP_PROJECT_ID)-

# Public bucket holding released, prebuilt plugin binaries (objects are
# <version>/nomad-nodepool-apm and <version>/nomad-deployment-aware-target).
# Published to by the release-please workflow on tagging a release;
# public-read, so downloads need no credentials.
E2B_ARTIFACT_BINARIES_URL ?= https://storage.googleapis.com/e2b-artifact-binaries/nomad-nodepool-apm

# Optional version "hook" for the released-artifact flow.
# When empty (default), build-and-upload builds from source and uploads the
# binaries to the client's fc-env-pipeline bucket (the existing flow,
# unchanged). When set (e.g. NOMAD_NODEPOOL_APM_VERSION=v0.1.0), skip
# building: download the released binaries and upload those instead. Both
# plugins release together under one version.
#
# Set it either on the command line or in the active .env.${ENV} file, which
# is included above. Quotes are stripped so both `=v0.1.0` and `="v0.1.0"`
# work; a command-line value always wins over the env file.
NOMAD_NODEPOOL_APM_VERSION := $(strip $(subst ",,$(NOMAD_NODEPOOL_APM_VERSION)))
ifneq ($(NOMAD_NODEPOOL_APM_VERSION),)
ifeq ($(filter v%,$(NOMAD_NODEPOOL_APM_VERSION)),)
$(error NOMAD_NODEPOOL_APM_VERSION must be a v-prefixed version (e.g. v0.1.0), not a commit SHA)
endif
endif

PLUGIN_NAME := nomad-nodepool-apm
TARGET_PLUGIN_NAME := nomad-deployment-aware-target
BIN_DIR := bin
BINARY := $(BIN_DIR)/$(PLUGIN_NAME)
TARGET_BINARY := $(BIN_DIR)/$(TARGET_PLUGIN_NAME)

.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BINARY) .
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(TARGET_BINARY) ./cmd/nomad-deployment-aware-target

.PHONY: clean
clean:
	rm -rf $(BIN_DIR)

.PHONY: test
test:
	go test -v ./...

.PHONY: upload
upload:
	chmod +x $(BINARY)
	chmod +x $(TARGET_BINARY)
ifeq ($(PROVIDER),aws)
	aws s3 cp $(BINARY) "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/nomad-nodepool-apm" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
	aws s3 cp $(TARGET_BINARY) "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/nomad-deployment-aware-target" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
ifneq ($(strip $(NOMAD_NODEPOOL_APM_VERSION)),)
	aws s3 cp $(BINARY) "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/nomad-nodepool-apm.$(NOMAD_NODEPOOL_APM_VERSION)" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
	aws s3 cp $(TARGET_BINARY) "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/nomad-deployment-aware-target.$(NOMAD_NODEPOOL_APM_VERSION)" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"
endif
else
	gcloud storage cp --cache-control="no-cache, max-age=0" $(BINARY) "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/nomad-nodepool-apm"
	gcloud storage cp --cache-control="no-cache, max-age=0" $(TARGET_BINARY) "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/nomad-deployment-aware-target"
ifneq ($(strip $(NOMAD_NODEPOOL_APM_VERSION)),)
	gcloud storage cp --cache-control="no-cache, max-age=0" $(BINARY) "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/nomad-nodepool-apm.$(NOMAD_NODEPOOL_APM_VERSION)"
	gcloud storage cp --cache-control="no-cache, max-age=0" $(TARGET_BINARY) "gs://${GCP_BUCKET_PREFIX}fc-env-pipeline/nomad-deployment-aware-target.$(NOMAD_NODEPOOL_APM_VERSION)"
endif
endif

# Released-artifact flow: download the prebuilt, versioned binaries from the
# public E2B artifact-binaries bucket.
.PHONY: pull-released
pull-released:
	@echo "Using released nomad-nodepool-apm $(NOMAD_NODEPOOL_APM_VERSION) from $(E2B_ARTIFACT_BINARIES_URL)"
	@mkdir -p $(BIN_DIR)
	curl -fL -o $(BINARY) "$(E2B_ARTIFACT_BINARIES_URL)/$(NOMAD_NODEPOOL_APM_VERSION)/$(PLUGIN_NAME)"
	curl -fL -o $(TARGET_BINARY) "$(E2B_ARTIFACT_BINARIES_URL)/$(NOMAD_NODEPOOL_APM_VERSION)/$(TARGET_PLUGIN_NAME)"

# The binaries come from source (default) or from released objects when
# NOMAD_NODEPOOL_APM_VERSION is set. Either way they land in ./bin and the
# upload target is identical.
ifeq ($(strip $(NOMAD_NODEPOOL_APM_VERSION)),)
BIN_SOURCE := build
else
BIN_SOURCE := pull-released
endif

.PHONY: build-and-upload
build-and-upload: $(BIN_SOURCE) upload
