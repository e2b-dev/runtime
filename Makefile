ENV := $(shell cat .last_used_env || echo "not-set")
ENV_FILE := .env.${ENV}
PROVIDER ?= gcp

-include ${ENV_FILE}

AWS_BUCKET_PREFIX ?= $(PREFIX)$(AWS_ACCOUNT_ID)-
GCP_BUCKET_PREFIX ?= $(GCP_PROJECT_ID)-

# Setup production environment variables, this is used only for E2B.dev production
# Uses Infisical CLI to read secrets from Infisical Vault
# To update them, use the Infisical UI directly
# On a first use, you need to run `infisical login` and `infisical init`
.PHONY: download-prod-env
download-prod-env:
	@  ./scripts/download-prod-env.sh ${ENV}

.PHONY: build
build/%:
	$(MAKE) -C packages/$(notdir $@) build

.PHONY: build-and-upload
build-and-upload:build-and-upload/api
build-and-upload:build-and-upload/client-proxy
build-and-upload:build-and-upload/dashboard-api
build-and-upload:build-and-upload/clean-nfs-cache
build-and-upload:build-and-upload/orchestrator
build-and-upload:build-and-upload/template-manager
build-and-upload:build-and-upload/envd
build-and-upload:build-and-upload/clickhouse-migrator
build-and-upload:build-and-upload/nomad-nodepool-apm
build-and-upload/clean-nfs-cache:
	./scripts/confirm.sh $(TERRAFORM_ENVIRONMENT)
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) $(MAKE) -C packages/orchestrator build-and-upload/clean-nfs-cache
build-and-upload/template-manager:
	./scripts/confirm.sh $(TERRAFORM_ENVIRONMENT)
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) $(MAKE) -C packages/orchestrator build-and-upload/template-manager
build-and-upload/orchestrator:
	./scripts/confirm.sh $(TERRAFORM_ENVIRONMENT)
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) $(MAKE) -C packages/orchestrator build-and-upload/orchestrator
build-and-upload/clickhouse-migrator:
	./scripts/confirm.sh $(TERRAFORM_ENVIRONMENT)
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) $(MAKE) -C packages/clickhouse build-and-upload
build-and-upload/%:
	./scripts/confirm.sh $(TERRAFORM_ENVIRONMENT)
	GCP_PROJECT_ID=$(GCP_PROJECT_ID) $(MAKE) -C packages/$(notdir $@) build-and-upload

.PHONY: copy-public-builds
copy-public-builds:
ifeq ($(PROVIDER),aws)
	mkdir -p ./.kernels
	mkdir -p ./.firecrackers
	mkdir -p ./.busybox
	aws s3 cp s3://e2b-artifact-binaries/kernels/ ./.kernels/ --recursive --no-sign-request --endpoint-url https://storage.googleapis.com
	aws s3 cp s3://e2b-artifact-binaries/firecrackers/ ./.firecrackers/ --recursive --no-sign-request --endpoint-url https://storage.googleapis.com
	aws s3 cp s3://e2b-artifact-binaries/busybox/ ./.busybox/ --recursive --no-sign-request --endpoint-url https://storage.googleapis.com
	aws s3 cp ./.kernels/ s3://${AWS_BUCKET_PREFIX}fc-kernels/ --recursive --profile ${AWS_PROFILE}
	aws s3 cp ./.firecrackers/ s3://${AWS_BUCKET_PREFIX}fc-versions/ --recursive --profile ${AWS_PROFILE}
	aws s3 cp ./.busybox/ s3://${AWS_BUCKET_PREFIX}fc-busybox/ --recursive --profile ${AWS_PROFILE}
	rm -rf ./.kernels
	rm -rf ./.firecrackers
	rm -rf ./.busybox
else ifeq ($(PROVIDER),azure)
	mkdir -p ./.kernels
	mkdir -p ./.firecrackers
	mkdir -p ./.busybox
	gcloud storage cp -r "gs://e2b-artifact-binaries/kernels/*" ./.kernels/
	gcloud storage cp -r "gs://e2b-artifact-binaries/firecrackers/*" ./.firecrackers/
	gcloud storage cp -r "gs://e2b-artifact-binaries/busybox/*" ./.busybox/
	az storage blob upload-batch --auth-mode login --overwrite --account-name $(AZURE_STORAGE_ACCOUNT_NAME) --destination fc-kernels --source ./.kernels
	az storage blob upload-batch --auth-mode login --overwrite --account-name $(AZURE_STORAGE_ACCOUNT_NAME) --destination fc-versions --source ./.firecrackers
	az storage blob upload-batch --auth-mode login --overwrite --account-name $(AZURE_STORAGE_ACCOUNT_NAME) --destination fc-busybox --source ./.busybox
	rm -rf ./.kernels
	rm -rf ./.firecrackers
	rm -rf ./.busybox
else
	gsutil cp -r gs://e2b-artifact-binaries/kernels/* gs://$(GCP_BUCKET_PREFIX)fc-kernels/
	gsutil cp -r gs://e2b-artifact-binaries/firecrackers/* gs://$(GCP_BUCKET_PREFIX)fc-versions/
	gsutil cp -r gs://e2b-artifact-binaries/busybox/* gs://$(GCP_BUCKET_PREFIX)fc-busybox/
endif

.PHONY: download-public-kernels
download-public-kernels:
	mkdir -p ./packages/fc-kernels
	gsutil cp -r gs://e2b-artifact-binaries/kernels/* ./packages/fc-kernels/

.PHONY: download-public-firecrackers
download-public-firecrackers:
	mkdir -p ./packages/fc-versions/builds/
	gsutil -m cp -r gs://e2b-artifact-binaries/firecrackers/* ./packages/fc-versions/builds/
	find ./packages/fc-versions/builds/ -name firecracker -exec chmod +x {} \;

.PHONY: generate
generate: generate/api generate/orchestrator generate/client-proxy generate/envd generate/db generate/shared generate/dashboard-api generate-tests generate-mocks
generate/%:
	@echo "Generating code for *$(notdir $@)*"
	$(MAKE) -C packages/$(notdir $@) generate
	@printf "\n\n"

.PHONY: generate-tests
generate-tests: generate-tests/integration
generate-tests/%:
	@echo "Generating code for *$(notdir $@)*"
	$(MAKE) -C tests/$(notdir $@) generate
	@printf "\n\n"

.PHONY: migrate
migrate:
	$(MAKE) -C packages/db migrate

.PHONY: prep-cluster
prep-cluster:
	$(MAKE) -C packages/shared prep-cluster

.PHONY: seed-db
seed-db:
	$(MAKE) -C packages/db seed-db

.PHONY: set-env
set-env:
	@ touch .last_used_env
	@ echo $(ENV) > .last_used_env
	@ . ./${ENV_FILE}

.PHONY: switch-env
switch-env:
	@ printf "Switching from `tput setaf 1``tput bold`$(shell cat .last_used_env)`tput sgr0` to `tput setaf 2``tput bold`$(ENV)`tput sgr0`\n\n"
	$(MAKE) set-env ENV=$(ENV)

.PHONY: test
test:
	go work edit -json \
		| jq -r '.Use[] | select (.DiskPath | contains("packages")) | .DiskPath' \
		| xargs -I{} $(MAKE) -C {} test

.PHONY: test-integration
test-integration:
	$(MAKE) -C tests/integration test-shard

.PHONY: fmt
fmt:
	golangci-lint fmt

.PHONY: lint
lint:
	go work edit -json | jq -r '.Use[].DiskPath' | xargs -P 4 -I{} golangci-lint run {}/... --fix

.PHONY: generate-mocks
generate-mocks:
	GOOS=linux mockery

.PHONY: tidy
tidy:
	@scripts/golang-dependencies-integrity.sh

.PHONY: local-infra
local-infra:
	$(MAKE) -C packages/local-dev local-infra
