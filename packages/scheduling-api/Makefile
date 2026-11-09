COMMIT_SHA ?= $(shell git rev-parse --short HEAD)

VERSION_LDFLAGS :=
ifneq ($(strip $(VERSION)),)
VERSION_LDFLAGS := -X=main.version=$(VERSION)
endif

.PHONY: generate
generate:
	go generate ./...

.PHONY: build
build:
	CGO_ENABLED=0 go build -o bin/scheduling-api -ldflags "-X=main.commitSHA=$(COMMIT_SHA) $(VERSION_LDFLAGS)" .

.PHONY: run-local
run-local: build
	NODE_ID=local ENVIRONMENT=local SCHEDULING_GRPC_ADDRESS=:50051 ./bin/scheduling-api

.PHONY: migrate
migrate:
	go run ./cmd/migrate

.PHONY: test
test:
	go test -race -v ./...

.PHONY: image
image:
	docker buildx build --load --build-arg COMMIT_SHA=$(COMMIT_SHA) -t scheduling-api:$(COMMIT_SHA) -f Dockerfile ..
