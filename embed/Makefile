SHELL := /bin/bash
.PHONY: lint config test images stores-check vector-validate sync-configs kubernetes-render terraform-validate

# Every compose invocation runs against compose/, where compose.yaml and .env
# live; the other two install shapes have their own directories beside it.
COMPOSE := docker compose --project-directory compose

SHELL_FILES := $(strip $(wildcard compose/scripts/*.sh))
# The bats files are linted too: shellcheck knows the bats shebang and reports
# SC2314 there, the check that catches a `! cmd` assertion which cannot fail a
# test. Leaving tests/ out of lint is how a vacuous assertion went unnoticed.
BATS_FILES := $(strip $(wildcard tests/*.bats))

config:
	$(COMPOSE) config -q

lint: config vector-validate kubernetes-render terraform-validate
	@if [ -n "$(strip $(SHELL_FILES))" ]; then shellcheck -x $(SHELL_FILES); fi
	@if [ -n "$(strip $(BATS_FILES))" ]; then shellcheck -x -s bash $(BATS_FILES); fi

kubernetes-render:
	kubectl kustomize kubernetes >/dev/null

# Both roots share one plugin cache, so the providers are downloaded once. The
# example is validated too: a renamed input or output is exactly how a
# copy-paste example goes stale.
terraform-validate:
	mkdir -p .terraform-cache
	TF_PLUGIN_CACHE_DIR=$(CURDIR)/.terraform-cache terraform -chdir=terraform/gcp init -backend=false -input=false >/dev/null
	terraform -chdir=terraform/gcp fmt -check -recursive
	terraform -chdir=terraform/gcp validate
	TF_PLUGIN_CACHE_DIR=$(CURDIR)/.terraform-cache terraform -chdir=terraform/gcp/examples/basic init -backend=false -input=false >/dev/null
	terraform -chdir=terraform/gcp/examples/basic validate

# Vector parses the config and compiles its VRL without connecting to sinks.
# Catches a broken remap or a misspelled sink option before a host does.
vector-validate:
	docker run --rm -v "$(CURDIR)/compose/config/vector/vector.toml:/etc/vector/vector.toml:ro" \
	  timberio/vector:0.51.1-alpine validate --no-environment /etc/vector/vector.toml

# After editing compose/config/clickhouse/config.xml or
# compose/config/vector/vector.toml: rewrite the inline copies in
# compose/compose.yaml and their SHA-256 stamps.
sync-configs:
	python3 compose/scripts/dev/sync-configs.py

test:
	@if [ -n "$$(ls tests/*.bats 2>/dev/null)" ]; then bats tests; else echo "no bats tests yet"; fi

# Build the three project images locally under their published names, so a
# compose.yaml that pins them runs the local build and pulls nothing. The
# seed builds from the public runtime repository at the pinned commit. The
# bake file lists both architectures; --load needs exactly one, the daemon's.
images:
	set -a; . ./compose/.env; set +a; \
	docker buildx bake --load \
	  --set "*.platform=linux/$$(docker version -f '{{.Server.Arch}}')" \
	  --set "seed.context=https://github.com/e2b-dev/runtime.git#$${RUNTIME_COMMIT}" \
	  --set seed.args.SRC=packages \
	  --set "tools.tags=$${E2B_TOOLS_IMAGE}" \
	  --set "node-e2b.tags=$${E2B_NODE_E2B_IMAGE}" \
	  --set "seed.tags=$${E2B_SEED_IMAGE}"

# The store-level integration check: both stores, their migrators, the seed,
# and the assertions that the seed row and both migration ledgers landed. It
# needs Docker and the images from `make images`, but no KVM, so it runs on a
# plain CI runner and on a developer's machine alike — the CI jobs are a
# checkout, `make images` and this target.
# Teardown is deliberately NOT part of this target: on a failure the containers
# stay up so the state can be inspected. Run
# `docker compose --project-directory compose down -v` when you are done (the
# workflows do that in an `if: always()` step).
# The FIX lines name where the evidence actually is. The one-shots run with
# `--rm` (as the CI job ran them), which creates `<project>-<service>-run-<hash>`
# containers that `docker compose logs <service>` does not include (every
# compose command here takes `--project-directory compose`) — so at the
# moment a one-shot fails its own log is EMPTY, and its output is what you just
# saw on stdout. `db-migrator` is the subtle one: it does get a real
# `e2b-db-migrator-1` container, but only later, when the seed's
# `service_completed_successfully` dependency starts it — so its log is empty at
# the run step below and populated by the time the ledger assertion runs, which
# is why only the assertion points at it. All three one-shots are idempotent, so
# re-running one is how you get its output back after an assertion fails.
stores-check:
	@set -e; \
	fail() { echo "stores-check: $$1" >&2; echo "FIX: $$2" >&2; exit 1; }; \
	$(COMPOSE) up -d --wait postgres clickhouse \
	  || fail "postgres and clickhouse did not become healthy" "run $(COMPOSE) logs postgres clickhouse"; \
	$(COMPOSE) run --rm db-migrator \
	  || fail "the postgres migrations failed" "read the db-migrator output above, and $(COMPOSE) logs postgres for the server side"; \
	$(COMPOSE) run --rm clickhouse-migrator \
	  || fail "the clickhouse migrations failed" "read the clickhouse-migrator output above, and $(COMPOSE) logs clickhouse for the server side"; \
	$(COMPOSE) run --rm seed \
	  || fail "the seed failed" "read the seed output above, and $(COMPOSE) logs db-migrator for the schema it seeds into"; \
	$(COMPOSE) run --rm --no-deps api-secrets \
	  || fail "api-secrets failed" "read the api-secrets output above"; \
	test "$$($(COMPOSE) run --rm --no-deps --entrypoint stat api-secrets -c %a /run/e2b/api.env 2>/dev/null)" = 600 \
	  || fail "/run/e2b/api.env is not mode 600" "re-run $(COMPOSE) run --rm --no-deps api-secrets and read its output"; \
	test "$$($(COMPOSE) run --rm --no-deps --entrypoint cat api-secrets /run/e2b/api.env 2>/dev/null | grep -c -E '^GEN_(ADMIN_TOKEN|SANDBOX_ACCESS_TOKEN_HASH_SEED)=[0-9a-f]{64}$$')" = 2 \
	  || fail "/run/e2b/api.env does not hold two 64-hex secrets" "the tools image's od/tr changed shape; re-run $(COMPOSE) run --rm --no-deps api-secrets and read the file"; \
	key="$$($(COMPOSE) run --rm --no-deps --entrypoint cat seed /run/e2b/team-api-key 2>/dev/null)" && [ -n "$$key" ] \
	  || fail "the seed left no /run/e2b/team-api-key" "the seed image must know SEED_TEAM_API_KEY_FILE (the published seed images do): for make images that is RUNTIME_COMMIT in compose/.env; read the seed output above"; \
	test "$$($(COMPOSE) exec -T postgres psql -U postgres -d postgres -tAc "SELECT count(*) FROM team_api_keys WHERE team_id='0b8a3ded-4489-4722-afd1-1d82e64ec2d5' AND api_key_hash = '\$$sha256\$$' || rtrim(encode(sha256(decode('$${key#e2b_}','hex')),'base64'),'=')")" = 1 \
	  || fail "the seeded api key is not the one in /run/e2b/team-api-key" "re-run $(COMPOSE) run --rm seed and compare its masked key with the file"; \
	test "$$($(COMPOSE) exec -T postgres psql -U postgres -d postgres -tAc "SELECT count(*) FROM team_api_keys WHERE team_id='0b8a3ded-4489-4722-afd1-1d82e64ec2d5'")" = 1 \
	  || fail "the seeded team has no api key" "re-run $(COMPOSE) run --rm seed and read its output"; \
	test "$$($(COMPOSE) exec -T postgres psql -U postgres -d postgres -tAc "SELECT count(*) > 0 FROM _migrations")" = t \
	  || fail "the postgres _migrations ledger is empty" "run $(COMPOSE) logs db-migrator"; \
	test "$$($(COMPOSE) exec -T clickhouse clickhouse-client --user clickhouse --password clickhouse --query 'SELECT count() > 0 FROM _migrations')" = 1 \
	  || fail "the clickhouse _migrations ledger is empty" "re-run $(COMPOSE) run --rm clickhouse-migrator and read its output"; \
	test "$$($(COMPOSE) exec -T clickhouse clickhouse-client --user clickhouse --password clickhouse --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'sandbox_logs'")" = 1 \
	  || fail "the sandbox_logs table is missing" "E2B_CLICKHOUSE_MIGRATOR_IMAGE must be a release that carries the add_sandbox_logs migration (the v0.4 line does); re-run $(COMPOSE) run --rm clickhouse-migrator"; \
	echo "stores-check: ok"
