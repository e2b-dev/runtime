ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}
PREFIX := $(strip $(subst ",,$(PREFIX)))

goose := GOOSE_DRIVER=postgres GOOSE_DBSTRING=$(POSTGRES_CONNECTION_STRING) go tool goose -table "_migrations" -dir "migrations"

# The migrator applies River's migrations after goose's; the goose CLI cannot.
migrator := go run ./scripts/migrator.go


.PHONY: migrate
migrate:
	@echo "Applying Postgres migration *$(notdir $@)* for ${ENV}"
	@POSTGRES_CONNECTION_STRING=$(POSTGRES_CONNECTION_STRING) $(migrator)
	@echo "Done"

.PHONY: migrate-local
migrate-local:
	@echo "Applying Postgres migration *$(notdir $@)* for ${ENV}"
	@POSTGRES_CONNECTION_STRING=postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable $(migrator)
	@echo "Done"

.PHONY: migrate/up
migrate/up: migrate

.PHONY: migrate/down
migrate/down:
	@echo "Reverting Postgres migration *$(notdir $@)* for ${ENV}"
	@$(goose) down
	@echo "Done"

.PHONY: build-debug
build-debug:
	go mod download
	go vet ./...

.PHONY: create-migration
create-migration:
ifeq ($(origin NAME), undefined)
	@echo "The expected syntax is: make create-migration NAME=your-migration-name"
	@exit 1
endif
	@$(goose) create $(NAME) sql

.PHONY: status
status:
	@$(goose) status

.PHONY: generate
generate:
	rm -rf queries/*.go
	go tool sqlc generate

.PHONY: build-tools
build-tools:
	@go tool goose --version > /dev/null

.PHONY: test
test: build-tools
	go test -v ./tests/...

.PHONY: seed-db
seed-db:
	@echo "Seeding database..."
	@POSTGRES_CONNECTION_STRING=$(POSTGRES_CONNECTION_STRING) go run ./scripts/seed/postgres/seed-db.go
