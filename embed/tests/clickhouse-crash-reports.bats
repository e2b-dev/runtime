#!/usr/bin/env bats

# The stock ClickHouse config sends a report to ClickHouse's developers when
# the server crashes. The config both shapes are built from turns that off;
# this keeps it off.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

@test "clickhouse sends no crash reports" {
  run tr -d ' \n' < compose/config/clickhouse/config.xml
  [[ "$output" == *"<send_crash_reports><enabled>false</enabled></send_crash_reports>"* ]]
}
