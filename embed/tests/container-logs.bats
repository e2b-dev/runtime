#!/usr/bin/env bats

# Docker's json-file driver keeps a container's log forever unless told
# otherwise. On a host whose disk has filled, a service that fails on every
# write, ClickHouse above all, writes error output faster than anything frees
# space, and the disk stays full until someone intervenes. Every service in
# compose.yaml therefore carries the shared `x-logging` cap. The render turns
# every profile on so a service behind one is held to it too.
#
# Like inline-configs.bats, this file needs the compose plugin (to render
# compose.yaml; no daemon is involved) and `jq` on PATH.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
  RENDERED_JSON="$(docker compose --project-directory compose --profile '*' config --format json)"
}

@test "every service caps its container log with the json-file size and file-count options" {
  run jq -r '.services | to_entries[]
    | select(.value.logging.driver != "json-file"
             or (.value.logging.options["max-size"] // "") == ""
             or (.value.logging.options["max-file"] // "") == "")
    | .key' <<<"$RENDERED_JSON"
  [ "$status" -eq 0 ]
  [ -z "$output" ] || { echo "services without the log cap: $output" >&2; return 1; }
}

# One anchor, aliased by every service, so a single edit moves the cap
# everywhere and no service can quietly carry a different one.
@test "every service aliases the one x-logging anchor" {
  services="$(jq -r '.services | length' <<<"$RENDERED_JSON")"
  [ "$services" -gt 0 ]
  [ "$(grep -c '^x-logging: &logging$' compose/compose.yaml)" -eq 1 ]
  aliases="$(grep -c '^    logging: \*logging$' compose/compose.yaml)"
  [ "$aliases" -eq "$services" ] || { echo "$aliases 'logging: *logging' lines for $services services" >&2; return 1; }
}
