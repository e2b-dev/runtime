#!/usr/bin/env bats

# The team API key is per install: the seed generates one (or takes
# TEAM_API_KEY from .env or the shell) and keeps it in the seed-state volume,
# where base-template, smoke and ready read it. No file in the install carries
# a key, so the install steps can be published without one.
#
# Like inline-configs.bats, the rendering tests need Docker with the compose
# plugin and jq on PATH. smoke is behind the test profile, so the render
# enables it.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
  RENDERED_JSON="$(docker compose --project-directory compose --profile test config --format json)"
}

@test "no shipped file carries a team key or a value for the api's two secrets" {
  # e2b_<32 hex> is the team-key shape. This is the one guard over the whole
  # tree; kubernetes.bats and terraform.bats do not repeat it.
  #
  # -ne 0 rather than -eq 1: with no match the trailing grep exits 1, which BSD
  # xargs reports as 1 and GNU xargs as 123. A match makes the pipeline 0, and
  # anything on stderr shows up in $output.
  run bash -c 'git ls-files -z | grep -zv "^tests/team-api-key.bats$" | xargs -0 grep -lE "e2b_[0-9a-f]{32}"'
  [ "$status" -ne 0 ]
  [ -z "$output" ]
  # The api's own two are per install in every shape as well: compose
  # generates them in api-secrets, Terraform writes generated ones into .env
  # at first boot, and Kubernetes reads the Secret the install creates. So no
  # file may carry a value for either, and a 64-hex literal beside one of the
  # two names is a shipped secret, whether it is a default, an example or a
  # commented-out line. This file names the shape rather than any value,
  # because the two development defaults that used to live in compose.yaml
  # are gone from the tree, this test included.
  run bash -c 'git ls-files -z | xargs -0 grep -lE "(ADMIN_TOKEN|SANDBOX_ACCESS_TOKEN_HASH_SEED)[=:].*[0-9a-f]{64}"'
  [ "$status" -ne 0 ]
  [ -z "$output" ]
}

@test "seed asks for a generated key kept in the shared file by default" {
  run bash -c 'echo "$1" | jq -r ".services.seed.environment.SEED_TEAM_API_KEY, .services.seed.environment.SEED_TEAM_API_KEY_FILE"' _ "$RENDERED_JSON"
  [ "$status" -eq 0 ]
  [ "$output" = $'random\n/run/e2b/team-api-key' ]
}

@test "TEAM_API_KEY in the environment pins the seed key" {
  run bash -c 'TEAM_API_KEY=e2b_ffeeddccbbaa99887766554433221100 docker compose --project-directory compose --profile test config --format json | jq -r ".services.seed.environment.SEED_TEAM_API_KEY"'
  [ "$status" -eq 0 ]
  [ "$output" = "e2b_ffeeddccbbaa99887766554433221100" ]
}

@test "seed, base-template, smoke and ready share the seed-state volume at /run/e2b" {
  run bash -c 'echo "$1" | jq -r "has(\"volumes\") and (.volumes | has(\"seed-state\"))"' _ "$RENDERED_JSON"
  [ "$output" = "true" ]
  for service in seed base-template smoke ready; do
    run bash -c 'echo "$1" | jq -r ".services[\"$2\"].volumes[]? | select(.target == \"/run/e2b\") | .source"' _ "$RENDERED_JSON" "$service"
    [ "$status" -eq 0 ]
    [ "$output" = "seed-state" ]
  done
}

@test "base-template, smoke and ready take the key from the seed file, not from compose/compose.yaml" {
  for service in base-template smoke ready; do
    run bash -c 'echo "$1" | jq -r ".services[\"$2\"].command | join(\" \")"' _ "$RENDERED_JSON" "$service"
    [ "$status" -eq 0 ]
    [[ "$output" == *"/run/e2b/team-api-key"* ]]
    run bash -c 'echo "$1" | jq -r ".services[\"$2\"].environment.E2B_API_KEY // empty"' _ "$RENDERED_JSON" "$service"
    [ -z "$output" ]
  done
}

@test "ready writes the SDK variables to /run/e2b/sdk.env for docker compose exec" {
  run bash -c 'echo "$1" | jq -r ".services.ready.command | join(\" \")"' _ "$RENDERED_JSON"
  [[ "$output" == *"/run/e2b/sdk.env"* ]]
}

@test "base-template waits for the seed directly, so a rotation up cannot race the key file" {
  run bash -c 'echo "$1" | jq -r ".services[\"base-template\"].depends_on.seed.condition"' _ "$RENDERED_JSON"
  [ "$status" -eq 0 ]
  [ "$output" = "service_completed_successfully" ]
}
