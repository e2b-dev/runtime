#!/usr/bin/env bats

# With CLICKHOUSE_DB set, the ClickHouse image runs a loopback-only server to
# create that database before it starts the real one. A localhost probe cannot
# tell the two apart, so a dependant could dial the service address while only
# the temporary server was up. The server creates `default` itself, so neither
# shape sets the variable, and these keep it out.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

@test "the compose clickhouse service sets no CLICKHOUSE_DB" {
  run grep -E '^ +CLICKHOUSE_DB:' compose/compose.yaml
  [ "$status" -eq 1 ]
}

@test "the kubernetes clickhouse container sets no CLICKHOUSE_DB" {
  run grep -E 'name: CLICKHOUSE_DB,' kubernetes/statefulset.yaml
  [ "$status" -eq 1 ]
}
