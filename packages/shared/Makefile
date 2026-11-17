.PHONY: generate
generate:
	go generate ./...

.PHONY: test
test:
	go test -v ./pkg/...

.PHONY: lint
lint:
	golangci-lint run --fix ./...
