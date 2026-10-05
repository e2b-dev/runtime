#!/usr/bin/env bats

# Replays fixture log lines through the shipped Vector configuration and
# asserts the ClickHouse `sandbox_logs` row each becomes, or that it is dropped.
# `compose/scripts/dev/vector-testconfig.py` copies
# compose/config/vector/vector.toml with the http_server source swapped for
# stdin and the ClickHouse sink for a JSON console sink; everything in between,
# the route included, is the real thing, and Vector compiles each remap on its
# own exactly as it does in the stack.
# The run uses the same Vector image `make vector-validate` uses. An accepted
# line prints one JSON row; a dropped line prints nothing. The stdin source adds
# a `host` field the http_server source does not, so `fields` carries one extra
# key here. Needs Docker, python3 and jq, like tests/inline-configs.bats.

# `run --separate-stderr`, used below to read Vector's exit status without
# folding its startup warnings into the rows.
bats_require_minimum_version 1.5.0

setup_file() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
  export CFG_DIR="$BATS_FILE_TMPDIR"
  python3 compose/scripts/dev/vector-testconfig.py > "$CFG_DIR/vector.toml"
  VECTOR_IMAGE="$(sed -n 's/.*\(timberio\/vector:[^ ]*\).*/\1/p' Makefile | head -1)"
  export VECTOR_IMAGE
  [ -n "$VECTOR_IMAGE" ]
}

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

# rows <fixture>: the JSON rows the fixture's lines become, one per line.
rows() {
  docker run --rm -i -v "$CFG_DIR:/cfg:ro" "$VECTOR_IMAGE" --config /cfg/vector.toml --quiet \
    < "$BATS_TEST_DIRNAME/fixtures/vector/$1" 2>/dev/null
}

# vector_run <fixture>: replays the fixture and leaves Vector's exit status in
# $status and its rows in $output. A dropped line prints nothing on stdout, and
# so does a run that never started, so a test that expects no rows has to check
# the status as well. --separate-stderr keeps Vector's startup warnings out of
# $output.
vector_run() {
  run --separate-stderr docker run --rm -i -v "$CFG_DIR:/cfg:ro" "$VECTOR_IMAGE" \
    --config /cfg/vector.toml --quiet \
    < "$BATS_TEST_DIRNAME/fixtures/vector/$1"
}

# col <row> <name>: a column of the row.
col() {
  printf '%s' "$1" | jq -r --arg k "$2" '.[$k]'
}

# demoted <row> <name>: true when the key ended up inside the fields column.
demoted() {
  printf '%s' "$1" | jq -r '.fields | fromjson | has($k)' --arg k "$2"
}

@test "an envd line becomes a row with the promoted columns and the rest in fields" {
  row="$(rows envd.json)"
  [ "$(col "$row" sandbox_id)" = "bnikj0253lrqkzi65evyj" ]
  [ "$(col "$row" team_id)" = "0b8a3ded-4489-4722-afd1-1d82e64ec2d5" ]
  [ "$(col "$row" template_id)" = "3tscgts0lhhzapkb11fr" ]
  [ "$(col "$row" build_id)" = "" ]
  [ "$(col "$row" service)" = "envd" ]
  [ "$(col "$row" category)" = "default" ]
  [ "$(col "$row" level)" = "info" ]
  [ "$(col "$row" message)" = "Process with pid 383 ended" ]
  [ "$(col "$row" timestamp)" = "2026-09-04T18:53:57.123456789Z" ]
  [ "$(demoted "$row" logger)" = "true" ]
  [ "$(demoted "$row" process_result)" = "true" ]
  [ "$(demoted "$row" teamID)" = "false" ]
  [ "$(demoted "$row" sandboxID)" = "false" ]
  [ "$(demoted "$row" message)" = "false" ]
  [ "$(printf '%s' "$row" | jq -r '.raw | fromjson | .teamID')" = "0b8a3ded-4489-4722-afd1-1d82e64ec2d5" ]
}

@test "a template-manager build line without a sandbox is kept as sandbox unknown with its build id" {
  row="$(rows template-manager-build.json)"
  [ "$(col "$row" sandbox_id)" = "unknown" ]
  [ "$(col "$row" build_id)" = "a932c9e4-78fc-46b3-b32c-e85ac5eeedde" ]
  [ "$(col "$row" service)" = "template-manager" ]
  [ "$(col "$row" template_id)" = "3tscgts0lhhzapkb11fr" ]
}

@test "dotted orchestrator keys are normalised and the level lowercased" {
  row="$(rows orchestrator-dotted-keys.json)"
  [ "$(col "$row" team_id)" = "0b8a3ded-4489-4722-afd1-1d82e64ec2d5" ]
  [ "$(col "$row" sandbox_id)" = "sbx-dotted-1" ]
  [ "$(col "$row" template_id)" = "tpl-dotted" ]
  [ "$(col "$row" level)" = "info" ]
}

@test "service falls back to service_name" {
  row="$(rows service-name-only.json)"
  [ "$(col "$row" service)" = "orchestrator" ]
  [ "$(col "$row" sandbox_id)" = "sbx-svc-1" ]
}

@test "an explicit sandboxID wins over instanceID" {
  [ "$(col "$(rows both-sandbox-ids.json)" sandbox_id)" = "sbx-explicit-9" ]
}

@test "unknown template and build ids become empty columns" {
  row="$(rows unknown-template-and-build.json)"
  [ "$(col "$row" template_id)" = "" ]
  [ "$(col "$row" build_id)" = "" ]
  [ "$(col "$row" level)" = "warn" ]
}

@test "a line whose team id is not a UUID is dropped" {
  vector_run team-not-uuid.json
  [ "$status" -eq 0 ] || { echo "vector exited $status: ${stderr:-}"; return 1; }
  [ -z "$output" ]
}

@test "a line with no sandbox id that is not a build line is dropped" {
  vector_run no-sandbox-not-build.json
  [ "$status" -eq 0 ] || { echo "vector exited $status: ${stderr:-}"; return 1; }
  [ -z "$output" ]
}

# sandbox_logs.timestamp is DateTime64(9): 1900-01-01 to 2262-04-11. A producer
# clock outside that range would make ClickHouse reject the whole batch, which
# the sink then retries forever, stalling every later line. Such a line keeps
# its content and takes the collector's time.
@test "a timestamp past 2262 is replaced with the collector's time, the line kept" {
  row="$(rows timestamp-past-2262.json)"
  [ "$(col "$row" message)" = "clock from the future" ]
  case "$(col "$row" timestamp)" in 2300-*) return 1 ;; 20[0-9][0-9]-*) ;; *) return 1 ;; esac
}

@test "a timestamp before 1900 is replaced with the collector's time, the line kept" {
  row="$(rows timestamp-before-1900.json)"
  [ "$(col "$row" message)" = "clock from the past" ]
  case "$(col "$row" timestamp)" in 1800-*) return 1 ;; 20[0-9][0-9]-*) ;; *) return 1 ;; esac
}
