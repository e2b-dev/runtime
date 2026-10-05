#!/usr/bin/env bats

# The api's two secrets, ADMIN_TOKEN and SANDBOX_ACCESS_TOKEN_HASH_SEED, are
# per install the way the team API key is: the api-secrets one-shot writes a
# generated pair into the seed-state volume on the first start, and the api's
# entrypoint fills in whichever of the two the operator did not set. So
# compose.yaml carries no value for either, and these tests hold that line: a
# default that grew back would put every install on one shared admin token
# again, and a broken entrypoint would silently drop the api's port argument.
# dashboard-api reads the same file for the same admin token, so its wrapper
# is held here too, beside the api's: tests/dashboard.bats reads none of it,
# and the whole entrypoint could be deleted with every dashboard test green.
#
# Rendering needs Docker with the compose plugin and jq, as team-api-key.bats
# does. The render runs with both variables unset, because a value exported in
# the test runner's own shell is exactly what the api reads, so it would
# otherwise be what these assertions see.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
  RENDERED_JSON="$(env -u ADMIN_TOKEN -u SANDBOX_ACCESS_TOKEN_HASH_SEED \
    docker compose --project-directory compose config --format json)"
}

# A shell snippet out of the rendered file. `docker compose config` writes a
# literal $ back out as $$ so that its output is a compose file again; undo
# that before reading one as shell, the way kubernetes.bats does.
unescaped() {
  echo "$RENDERED_JSON" | jq -r "$1" | sed 's/\$\$/$/g'
}

@test "compose.yaml ships no value for either api secret" {
  for var in ADMIN_TOKEN SANDBOX_ACCESS_TOKEN_HASH_SEED; do
    run bash -c 'echo "$1" | jq -r ".services.api.environment | has(\"$2\")"' _ "$RENDERED_JSON" "$var"
    [ "$output" = "true" ] || { echo "the api does not declare $var"; return 1; }
    # An empty string, and not null: a null value is Compose's "take it from
    # the host environment", which would leave the variable out of the
    # container altogether and hide the generated pair behind whatever the
    # operator happens to export.
    run bash -c 'echo "$1" | jq -r ".services.api.environment[\"$2\"] | type, length"' _ "$RENDERED_JSON" "$var"
    [ "$output" = $'string\n0' ] || { echo "$var renders as '$output', want an empty string"; return 1; }
  done
}

@test "a value in the environment overrides one secret and leaves the other generated" {
  run bash -c 'env -u SANDBOX_ACCESS_TOKEN_HASH_SEED ADMIN_TOKEN=operator docker compose --project-directory compose config --format json | jq -c "[.services.api.environment.ADMIN_TOKEN, .services.api.environment.SANDBOX_ACCESS_TOKEN_HASH_SEED]"'
  [ "$status" -eq 0 ]
  [ "$output" = '["operator",""]' ]
}

@test "the api reads the pair from the seed-state volume, mounted read-only" {
  run bash -c 'echo "$1" | jq -r ".services.api.volumes[] | select(.target == \"/run/e2b\") | \"\(.source) \(.read_only)\""' _ "$RENDERED_JSON"
  [ "$output" = "seed-state true" ]
  run bash -c 'echo "$1" | jq -r ".services.api.depends_on[\"api-secrets\"].condition"' _ "$RENDERED_JSON"
  [ "$output" = "service_completed_successfully" ]
}

@test "the api entrypoint takes each secret from the file only when it is unset" {
  local script
  script="$(unescaped '.services.api.entrypoint[2]')"
  [[ "$script" == *". /run/e2b/api.env"* ]]
  # shellcheck disable=SC2016  # the two expansions are the api's own shell, matched literally
  [[ "$script" == *'${ADMIN_TOKEN:-$GEN_ADMIN_TOKEN}'* ]]
  # shellcheck disable=SC2016
  [[ "$script" == *'${SANDBOX_ACCESS_TOKEN_HASH_SEED:-$GEN_SANDBOX_ACCESS_TOKEN_HASH_SEED}'* ]]
  # exec, so the api stays PID 1 and its exit code is the container's; "$@"
  # with the placeholder below, so `command:` still reaches the binary.
  # shellcheck disable=SC2016
  [[ "$script" == *'exec ./api "$@"'* ]]
  # `sh -c` takes the first word after the script as $0. Without the
  # placeholder the port flag would land there and the api would listen on
  # its own default while the healthcheck polls 3000.
  run bash -c 'echo "$1" | jq -r ".services.api.entrypoint | length, .[-1]"' _ "$RENDERED_JSON"
  [ "$output" = $'4\napi' ]
  run bash -c 'echo "$1" | jq -r ".services.api.command | join(\" \")"' _ "$RENDERED_JSON"
  [ "$output" = "--port 3000" ]
}

# The same wrapper as the api's, with one secret instead of two, so the two
# processes hold the one admin token this install generated.
@test "the dashboard-api entrypoint takes the admin token only when it is unset" {
  local script
  script="$(unescaped '.services["dashboard-api"].entrypoint[2]')"
  [[ "$script" == *". /run/e2b/api.env"* ]]
  # shellcheck disable=SC2016  # the expansion is dashboard-api's own shell
  [[ "$script" == *'${ADMIN_TOKEN:-$GEN_ADMIN_TOKEN}'* ]]
  # shellcheck disable=SC2016
  [[ "$script" == *'[ -n "${GEN_ADMIN_TOKEN:-}" ]'* ]]
  # One secret and not two: the sandbox seed is the api's alone, and a wrapper
  # copied wholesale from the api would make this service refuse to start
  # whenever that half of the file is missing.
  [[ "$script" != *GEN_SANDBOX_ACCESS_TOKEN_HASH_SEED* ]]
  # Relative, because the image's working directory is / and its own
  # entrypoint is ./dashboard-api, exactly as the api's wrapper is.
  # shellcheck disable=SC2016
  [[ "$script" == *'exec ./dashboard-api "$@"'* ]]
  # The $0 placeholder, as above. This service passes no `command:` today, so
  # nothing is dropped yet; without the placeholder the first argument added
  # later would vanish instead of reaching the binary.
  run bash -c 'echo "$1" | jq -r ".services[\"dashboard-api\"].entrypoint | length, .[-1]"' _ "$RENDERED_JSON"
  [ "$output" = $'4\ndashboard-api' ]
}

@test "api-secrets writes the pair into the shared volume, gated only on preflight" {
  run bash -c 'echo "$1" | jq -r ".services[\"api-secrets\"].volumes[] | select(.target == \"/run/e2b\") | \"\(.source) \(.read_only == true)\""' _ "$RENDERED_JSON"
  [ "$output" = "seed-state false" ]
  # preflight and nothing else: the pair needs no database and no host
  # preparation, and it is the api that waits for it.
  run bash -c 'echo "$1" | jq -r ".services[\"api-secrets\"].depends_on | keys | join(\",\")"' _ "$RENDERED_JSON"
  [ "$output" = "preflight" ]
  run bash -c 'echo "$1" | jq -r ".services[\"api-secrets\"].depends_on.preflight.condition"' _ "$RENDERED_JSON"
  [ "$output" = "service_completed_successfully" ]
  run bash -c 'echo "$1" | jq -r ".services[\"api-secrets\"].restart"' _ "$RENDERED_JSON"
  [ "$output" = "no" ]
}

@test "api-secrets keeps an existing pair, writes a private file and prints no value" {
  local script
  script="$(unescaped '.services["api-secrets"].command[2]')"
  [[ "$script" == *"reusing /run/e2b/api.env"* ]]
  [[ "$script" == *"GEN_ADMIN_TOKEN="* ]]
  [[ "$script" == *"GEN_SANDBOX_ACCESS_TOKEN_HASH_SEED="* ]]
  # Written 600 under umask 077, to a temp file that is moved into place, so a
  # reader either sees the whole pair or no file at all.
  [[ "$script" == *"umask 077"* ]]
  [[ "$script" == *"chmod 600"* ]]
  # shellcheck disable=SC2016  # the $tmp is the one-shot's own shell
  [[ "$script" == *'> "$tmp"'* ]]
  [[ "$script" == *"mv "* ]]
  # The values reach that redirection and nothing else: an echo carrying an
  # expansion would put the admin token in `docker compose logs`.
  run bash -c 'printf "%s\n" "$1" | grep -nE "^[[:space:]]*echo .*[$]"' _ "$script"
  [ "$status" -eq 1 ]
  [ -z "$output" ]
}

# The parity and content tests above read each snippet as text, so a syntax
# error in any of them passes them. `/bin/sh` is what runs all three; `-n` is
# that same parser, stopped before it does anything.
@test "every secret snippet parses" {
  unescaped '.services["api-secrets"].command[2]' > "$BATS_TEST_TMPDIR/api-secrets"
  unescaped '.services.api.entrypoint[2]' > "$BATS_TEST_TMPDIR/api-entrypoint"
  unescaped '.services["dashboard-api"].entrypoint[2]' \
    > "$BATS_TEST_TMPDIR/dashboard-api-entrypoint"
  local snippet
  for snippet in api-secrets api-entrypoint dashboard-api-entrypoint; do
    [ -s "$BATS_TEST_TMPDIR/$snippet" ]
    # jq -r prints "null" for a snippet that is not there at all, and `sh -n`
    # reads that as a command name and exits 0. Without this line the loop
    # would pass for a service whose entrypoint had been deleted outright.
    [ "$(cat "$BATS_TEST_TMPDIR/$snippet")" != "null" ] || {
      echo "the $snippet snippet is absent from the rendered file"; return 1; }
    run sh -n "$BATS_TEST_TMPDIR/$snippet"
    [ "$status" -eq 0 ] || { echo "the $snippet snippet does not parse:"; echo "$output"; return 1; }
  done
}
