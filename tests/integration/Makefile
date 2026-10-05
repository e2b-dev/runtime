ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

# Max in-flight parallel tests per package (go test -parallel). Measurements
# on the CI hosts showed they saturate server-side (Firecracker boots + zstd
# compression) right around 4; higher values only inflate per-test times and
# flake template builds. Kept as a knob for local runs and future tuning.
TEST_PARALLELISM ?= 4

# Package shards so CI can split the suite across parallel runners; the wall
# clock is bound by the host (see TEST_PARALLELISM above), so the only way to
# go faster is more hosts. Shards are sized by measured test time:
#
#   - The ^TestTemplateBuild tests (real Firecracker builds, ~1015s of summed
#     test+subtest time measured uncompressed on run 30634340619) are split
#     into two roughly equal halves by name prefix: the shard-1 prefixes sum
#     to ~504s vs ~511s for the rest. New TestTemplateBuild* tests fall into
#     shard 2 unless they match the shard 1 prefix; rebalance by moving a
#     prefix when the shards' test steps drift apart.
#   - api/sandboxes gets its own runner, topped up with the light packages
#     (proxies, api/metrics, api/volumes) to balance against the rest shard.
#   - Everything else (including the non-build template tests) shares the
#     last runner; the -skip only affects the templates package since no
#     other package has TestTemplateBuild* tests.
TEMPLATE_BUILDS_SHARD1_RE := ^TestTemplateBuild(ENV|COPY|RUN|W|Cache|Fuse|Unsupported)
# Tests outside the TestTemplateBuild* family that build a template too: the
# tag, delete and assignment flows in api/templates and envd's CA-cert and
# process tests. In the rest shard each cost 65-190 s (run 34681252486) and
# together they bounded it; next to the other builds they share that shard's
# build tuning instead. Exact names, so a new test lands in rest until it is
# listed here on purpose.
TEMPLATE_BUILDING_TESTS_1_RE := ^Test(TagReassignment|DeleteTemplate|MultipleTagsOnSameTemplate|TemplateTagAssign|AssignmentOrdering|SandboxCreateWithTag|SandboxCreateWithAliasAndTag)
TEMPLATE_BUILDING_TESTS_2_RE := ^Test(CACert|CommandKillNextApp)
SANDBOXES_SHARD_PACKAGES := ./internal/tests/api/sandboxes/... ./internal/tests/api/metrics/... ./internal/tests/api/volumes/... ./internal/tests/proxies/...
TEST_SHARD ?= all
TEST_RUN_FLAGS :=
# TESTS_ONLY (the compression allow-list) composes with package-pure shards:
# its selection becomes a -run regex, so it cannot combine with the
# name-split shards that carry their own -run (go test keeps only the last
# -run flag, which would silently drop one of the two selections).
TESTS_ONLY_OK := yes
ifeq ($(TEST_SHARD),templates)
TEST_PACKAGES := ./internal/tests/api/templates/...
else ifeq ($(TEST_SHARD),no-templates)
# Package-level complement of the templates shard; computed via go list so
# new packages are picked up automatically. Used to split the compressed
# allow-list runs: templates (real-build snapshot tests) vs everything else.
TEST_PACKAGES = $(shell go list ./internal/tests/... | grep -v -e '/tests/api/templates')
else ifeq ($(TEST_SHARD),templates-builds-1)
TEST_PACKAGES := ./internal/tests/api/templates/...
TEST_RUN_FLAGS := -run '$(TEMPLATE_BUILDS_SHARD1_RE)|$(TEMPLATE_BUILDING_TESTS_1_RE)'
TESTS_ONLY_OK := no
else ifeq ($(TEST_SHARD),templates-builds-2)
TEST_PACKAGES := ./internal/tests/api/templates/... ./internal/tests/envd/...
TEST_RUN_FLAGS := -run '^TestTemplateBuild|$(TEMPLATE_BUILDING_TESTS_2_RE)' -skip '$(TEMPLATE_BUILDS_SHARD1_RE)'
TESTS_ONLY_OK := no
else ifeq ($(TEST_SHARD),sandboxes)
TEST_PACKAGES := $(SANDBOXES_SHARD_PACKAGES)
else ifeq ($(TEST_SHARD),rest)
# Everything not covered by the dedicated shards; computed via go list so
# new packages are picked up automatically.
TEST_PACKAGES = $(shell go list ./internal/tests/... | grep -v -e '/tests/api/sandboxes' -e '/tests/api/metrics' -e '/tests/api/volumes' -e '/tests/proxies')
TEST_RUN_FLAGS := -skip '^TestTemplateBuild|$(TEMPLATE_BUILDING_TESTS_1_RE)|$(TEMPLATE_BUILDING_TESTS_2_RE)'
else ifeq ($(TEST_SHARD),all)
TEST_PACKAGES := ./internal/tests/...
else
# Unknown shard names resolve to nothing so the guard in test-shard fails
# loudly instead of silently running the whole suite.
TEST_PACKAGES :=
endif

.PHONY: generate
generate:
	go generate ./...

.PHONY: build-debug
build-debug:
	go mod download
	go vet ./internal/...

.PHONY: seed
seed:
	@echo "Applying seeds"
	@POSTGRES_CONNECTION_STRING=$(POSTGRES_CONNECTION_STRING) \
		TESTS_E2B_API_KEY=$(TESTS_E2B_API_KEY) \
		TESTS_SANDBOX_TEMPLATE_ID=$(TESTS_SANDBOX_TEMPLATE_ID) \
		TESTS_SANDBOX_BUILD_ID=$(TESTS_SANDBOX_BUILD_ID) \
		TESTS_SANDBOX_RAM_MB=$(TESTS_SANDBOX_RAM_MB) \
		go run seed.go
	@echo "Done"

# TESTS_ONLY points at an allow-list file (see scripts/compression-tests.tsv)
# and narrows the run to the tests it names. Overridable from the environment.
TESTS_ONLY := $(strip $(TESTS_ONLY))

# The compressed configs only run what the allow-list names, so a stale entry
# would silently stop testing something. select-tests.sh rejects entries that
# match no test; this proves it still does, for named tests and for the
# package wildcards alike, before CI relies on it.
.PHONY: check-tests-allowlist
check-tests-allowlist:
	@set -e; \
	tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	./scripts/select-tests.sh --only scripts/compression-tests.tsv ./internal/tests/... >/dev/null; \
	printf 'internal/tests/orchestrator\t*\ninternal/tests/api/sandboxes\tTestNoSuchTestExists\n' >"$$tmp/named.tsv"; \
	printf 'internal/tests/orchestrator\t*\ninternal/tests/no-such-package\t*\n' >"$$tmp/wildcard.tsv"; \
	for kind in named wildcard; do \
		if ./scripts/select-tests.sh --only "$$tmp/$$kind.tsv" ./internal/tests/... >/dev/null 2>&1; then \
			echo "select-tests.sh accepted a stale $$kind entry" >&2; exit 1; \
		fi; \
	done; \
	echo "allow-list resolves; stale named and wildcard entries are rejected"

# CI entrypoint: run one package shard (or the whole suite with the default
# TEST_SHARD=all). TESTS_ONLY narrows the run to the allow-list within the
# shard's packages; the guard below rejects the name-split shards whose own
# -run flag would silently collide with the allow-list's.
.PHONY: test-shard
test-shard:
	@if [ -z "$(strip $(TEST_PACKAGES))" ]; then echo "no packages resolved for TEST_SHARD=$(TEST_SHARD)"; exit 1; fi
	@if [ -n "$(TESTS_ONLY)" ] && [ "$(TESTS_ONLY_OK)" != "yes" ]; then echo "TESTS_ONLY cannot compose with name-split shard $(TEST_SHARD) (-run flags collide)"; exit 1; fi
	@export POSTGRES_CONNECTION_STRING=$(POSTGRES_CONNECTION_STRING); \
	export TESTS_API_SERVER_URL=$(TESTS_API_SERVER_URL); \
	export TESTS_ORCHESTRATOR_HOST=$(TESTS_ORCHESTRATOR_HOST); \
	export TESTS_ENVD_PROXY=$(TESTS_ENVD_PROXY); \
	export TESTS_SANDBOX_TEMPLATE_ID=$(TESTS_SANDBOX_TEMPLATE_ID); \
	export TESTS_E2B_API_KEY=$(TESTS_E2B_API_KEY); \
	export TESTS_SANDBOX_TEAM_ID=$(TESTS_SANDBOX_TEAM_ID); \
	export TESTS_SANDBOX_USER_ID=$(TESTS_SANDBOX_USER_ID); \
	go test -v ./internal/main_test.go -count=1 && \
	RUN=""; \
	if [ -n "$(TESTS_ONLY)" ]; then RUN=$$(./scripts/select-tests.sh --only $(TESTS_ONLY) ./internal/tests/...) || exit 1; fi; \
	if [ -n "$${RUN}" ]; then set -- -run "$${RUN}"; else set --; fi; \
	go tool gotestsum --rerun-fails=1 --packages="$(strip $(TEST_PACKAGES))" --format standard-verbose --junitfile=test-results.xml \
		-- -count=1 -parallel=$(TEST_PARALLELISM) -timeout=20m $(TEST_RUN_FLAGS) "$$@"

.PHONY: test
test: test/.
test/%:
	@export POSTGRES_CONNECTION_STRING=$(POSTGRES_CONNECTION_STRING); \
	export TESTS_API_SERVER_URL=$(TESTS_API_SERVER_URL); \
	export TESTS_ORCHESTRATOR_HOST=$(TESTS_ORCHESTRATOR_HOST); \
	export TESTS_ENVD_PROXY=$(TESTS_ENVD_PROXY); \
	export TESTS_SANDBOX_TEMPLATE_ID=$(TESTS_SANDBOX_TEMPLATE_ID); \
	export TESTS_E2B_API_KEY=$(TESTS_E2B_API_KEY); \
	export TESTS_SANDBOX_TEAM_ID=$(TESTS_SANDBOX_TEAM_ID); \
	export TESTS_SANDBOX_USER_ID=$(TESTS_SANDBOX_USER_ID); \
	go test -v ./internal/main_test.go -count=1 && \
	TEST_PATH="./internal/tests/$(subst test/,,$@)"; \
	TEST_PATH="$${TEST_PATH%/.}"; \
	RUN=""; \
	case "$${TEST_PATH}" in \
		*.go:*) PACKAGES="$${TEST_PATH%%:*}"; RUN="$${TEST_PATH#*:}" ;; \
		*.go) PACKAGES="$${TEST_PATH}" ;; \
		*) PACKAGES="$${TEST_PATH}/..." ;; \
	esac; \
	if [ -n "$(TESTS_ONLY)" ]; then RUN=$$(./scripts/select-tests.sh --only $(TESTS_ONLY) "$${PACKAGES}") || exit 1; fi; \
	if [ -n "$${RUN}" ]; then set -- -run "$${RUN}"; else set --; fi; \
	go tool gotestsum --rerun-fails=1 --packages="$${PACKAGES}" --format standard-verbose --junitfile=test-results.xml \
		-- -count=1 -parallel=$(TEST_PARALLELISM) -timeout=20m "$$@"
