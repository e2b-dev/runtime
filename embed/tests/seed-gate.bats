#!/usr/bin/env bats

# compose/compose.yaml hands the seed SEED_TEAM_API_KEY and SEED_TEAM_API_KEY_FILE and
# expects the key file to exist afterwards. The seed at the RUNTIME_COMMIT
# this stack started with (12ee9b10) ignores both and leaves no file, so
# base-template refuses to start with a FIX line. The pin moved off it on
# 2026-09-04, onto a runtime commit whose seed knows the file, and reads
# 7278c2a3 today; this test keeps it from sliding back. Same shape as
# pins.bats.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

@test "RUNTIME_COMMIT is a seed that knows SEED_TEAM_API_KEY_FILE" {
  run grep -E '^RUNTIME_COMMIT=12ee9b1062d59a76c5d6b2df26e3e53c66fdbc08$' compose/.env
  [ "$status" -eq 1 ]
}
