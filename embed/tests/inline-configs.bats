#!/usr/bin/env bats

# Proves the inline `configs:` content in compose/compose.yaml is kept in sync
# with the source copies under compose/config/. jq -j (not -r) is used because -r always
# appends its own trailing newline to whatever it prints: for a config with no
# trailing newline (clickhouse's config.xml) that would make the diff show a
# spurious final-newline difference, and for one that already ends in a single
# newline (vector.toml) it would show a spurious trailing blank line. -j joins
# raw output with no added newline, so the rendered content compares byte for
# byte against the file on disk.
#
# Unlike the rest of tests/, this file needs a working Docker daemon with the
# compose plugin (to render compose/compose.yaml) and `jq` on PATH. GitHub's ubuntu
# runners ship both, which is why `make test` runs it there; a runner without
# them fails these tests in setup(). The same requirement is in the
# README's developer section.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
  RENDERED_JSON="$(docker compose --project-directory compose config --format json)"
}

@test "clickhouse-config inline content matches compose/config/clickhouse/config.xml byte for byte" {
  run bash -c 'diff <(echo "$1" | jq -j ".configs[\"clickhouse-config\"].content") compose/config/clickhouse/config.xml' _ "$RENDERED_JSON"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

@test "vector-config inline content matches compose/config/vector/vector.toml byte for byte" {
  run bash -c 'diff <(echo "$1" | jq -j ".configs[\"vector-config\"].content") compose/config/vector/vector.toml' _ "$RENDERED_JSON"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}

# Rendered with the otel profile on: the collector that mounts this config is
# behind it.
@test "otel-collector-config inline content matches compose/config/otel/otel-collector.yaml byte for byte" {
  rendered="$(docker compose --project-directory compose --profile otel config --format json)"
  run bash -c 'diff <(echo "$1" | jq -j ".configs[\"otel-collector-config\"].content") compose/config/otel/otel-collector.yaml' _ "$rendered"
  [ "$status" -eq 0 ]
  [ -z "$output" ]
}
