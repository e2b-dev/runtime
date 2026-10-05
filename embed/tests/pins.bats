#!/usr/bin/env bats

# Sandbox and build logs on ClickHouse need an api that reads LOGS_READ_CONFIG
# and accepts a missing LOKI_URL: api v0.11.0 (2026-09-05). The earlier pin
# v0.8.0 has neither and exits at startup, so a compose file that sets the
# override must never ship with it. This test failed on purpose until the pins
# moved; it now keeps them from sliding back below that version.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
}

@test "api pin is at least v0.11.0, the release that knows LOGS_READ_CONFIG" {
  grep -q 'LOGS_READ_CONFIG: "true"' compose/compose.yaml
  api_tag="$(sed -n 's/^E2B_API_IMAGE=.*:\(v[0-9.]*\).*/\1/p' compose/.env)"
  [ -n "$api_tag" ]
  min=v0.11.0
  lowest="$(printf '%s\n%s\n' "$min" "$api_tag" | sort -V | head -1)"
  if [ "$lowest" != "$min" ]; then
    echo "E2B_API_IMAGE is $api_tag; optional LOKI_URL and LOGS_READ_CONFIG shipped in api $min, and older pins exit on the missing LOKI_URL" >&2
    return 1
  fi
}

# The whole tag, not the SemVer prefix: an auto-deploy tag carries the commit
# after a hyphen, and two images from different commits must not compare equal.
@test "api and db-migrator pins move together" {
  api_tag="$(sed -n 's/^E2B_API_IMAGE=.*:\(v[^ #]*\).*/\1/p' compose/.env)"
  mig_tag="$(sed -n 's/^E2B_DB_MIGRATOR_IMAGE=.*:\(v[^ #]*\).*/\1/p' compose/.env)"
  [ -n "$api_tag" ]
  [ "$api_tag" = "$mig_tag" ]
}

# The three stack images are named in three places that no single edit keeps in
# step: docker-bake.hcl builds them into REGISTRY_PREFIX, compose/.env pins them
# for the Compose and Terraform shapes, and kubernetes/kustomization.yaml
# repeats the same pins for the Kubernetes shape. A registry repository renamed
# in the bake file alone publishes the images somewhere the two installs do not
# pull from, and a tag bumped in one install file alone runs two shapes on
# different code. These are the tests that read all three files together.

# The three stack-image variables, in the order the two install files list them.
STACK_VARS=(E2B_TOOLS_IMAGE E2B_NODE_E2B_IMAGE E2B_SEED_IMAGE)

# env_pin <variable>: the value compose/.env assigns it, without the trailing
# release marker comment the stack-image lines carry.
env_pin() {
  sed -n "s|^$1=\([^ #]*\).*|\1|p" compose/.env
}

# k8s_pin <variable>: the newName:newTag the kustomization gives that image
# name. newName sorts before newTag, so the last name seen is this entry's.
k8s_pin() {
  awk -v want="$1" '
    /^ *- name: / { entry = $3 }
    /^ *newName: / { name = $2 }
    /^ *newTag: / { if (entry == want) print name ":" $2 }
  ' kubernetes/kustomization.yaml
}

# bake_repository: the one registry repository docker-bake.hcl defaults to.
bake_repository() {
  sed -n 's|^ *default *= *"us-docker\.pkg\.dev/e2b-artifacts/\([^"/]*\)" *$|\1|p' docker-bake.hcl
}

@test "docker-bake.hcl declares exactly one registry repository" {
  run bake_repository
  [ "$status" -eq 0 ]
  [ "$(printf '%s\n' "$output" | grep -c .)" -eq 1 ]
}

@test "the three stack-image pins name the registry repository the bake builds into" {
  local repository var pin
  repository="$(bake_repository)"
  [ -n "$repository" ]
  for var in "${STACK_VARS[@]}"; do
    for pin in "$(env_pin "$var")" "$(k8s_pin "$var")"; do
      case "$pin" in
        "us-docker.pkg.dev/e2b-artifacts/${repository}/"*:*) ;;
        *)
          echo "$var pins '$pin'; docker-bake.hcl builds into us-docker.pkg.dev/e2b-artifacts/$repository" >&2
          return 1
          ;;
      esac
    done
  done
}

@test "each compose/.env stack pin equals its kustomization counterpart" {
  local var env k8s
  for var in "${STACK_VARS[@]}"; do
    env="$(env_pin "$var")"
    k8s="$(k8s_pin "$var")"
    [ -n "$env" ] || { echo "compose/.env has no $var" >&2; return 1; }
    [ -n "$k8s" ] || { echo "kubernetes/kustomization.yaml has no $var entry" >&2; return 1; }
    if [ "$env" != "$k8s" ]; then
      echo "$var is $env in compose/.env and $k8s in kubernetes/kustomization.yaml" >&2
      return 1
    fi
  done
}

@test "the two install files pin all three stack images and no others into that repository" {
  local repository
  repository="$(bake_repository)"
  [ "$(grep -cE "^[A-Z0-9_]+=us-docker\.pkg\.dev/e2b-artifacts/${repository}/" compose/.env)" -eq 3 ]
  [ "$(grep -cE "^ *newName: us-docker\.pkg\.dev/e2b-artifacts/${repository}/" kubernetes/kustomization.yaml)" -eq 3 ]
}

@test "the three stack images carry the same tag" {
  local var pin tag first=""
  for var in "${STACK_VARS[@]}"; do
    pin="$(env_pin "$var")"
    [ -n "$pin" ] || { echo "compose/.env has no $var" >&2; return 1; }
    tag="${pin##*:}"
    if [ -z "$first" ]; then first="$tag"; continue; fi
    if [ "$tag" != "$first" ]; then
      echo "$var is tagged $tag, the first stack image $first; the three move together" >&2
      return 1
    fi
  done
}

# The dashboard is two more pins. dashboard-api is a platform image released
# with the group, so its line carries the release marker and must be built
# from the same source commit as the api it serves (the auto-deploy tag ends
# in that commit). The dashboard image has its own release line outside the
# group, so its line carries no marker and is bumped by hand.
DASHBOARD_VARS=(E2B_DASHBOARD_API_IMAGE E2B_DASHBOARD_IMAGE)

@test "the two dashboard pins are in both install files and equal" {
  local var env k8s
  for var in "${DASHBOARD_VARS[@]}"; do
    env="$(env_pin "$var")"
    k8s="$(k8s_pin "$var")"
    [ -n "$env" ] || { echo "compose/.env has no $var" >&2; return 1; }
    [ -n "$k8s" ] || { echo "kubernetes/kustomization.yaml has no $var entry" >&2; return 1; }
    [ "$env" = "$k8s" ] || { echo "$var is $env in compose/.env and $k8s in kubernetes/kustomization.yaml" >&2; return 1; }
  done
}

@test "dashboard-api is built from the api's source commit" {
  api_tag="$(sed -n 's/^E2B_API_IMAGE=.*:\(v[^ #]*\).*/\1/p' compose/.env)"
  dash_tag="$(sed -n 's/^E2B_DASHBOARD_API_IMAGE=.*:\(v[^ #]*\).*/\1/p' compose/.env)"
  [ -n "$api_tag" ] && [ -n "$dash_tag" ]
  [ "${api_tag##*-}" = "${dash_tag##*-}" ] || {
    echo "api is built from ${api_tag##*-} and dashboard-api from ${dash_tag##*-}; dashboard-api checks the schema level its api's migrator applied, so the two move together" >&2
    return 1
  }
}

@test "the dashboard-api pin carries the release marker and the dashboard pin does not" {
  grep -qE '^E2B_DASHBOARD_API_IMAGE=.* # x-release-please-version$' compose/.env
  grep -qE '^E2B_DASHBOARD_IMAGE=us-docker\.pkg\.dev/e2b-artifacts/dashboard/dashboard:v[0-9]+\.[0-9]+\.[0-9]+$' compose/.env
  grep -A2 '^  - name: E2B_DASHBOARD_API_IMAGE$' kubernetes/kustomization.yaml | grep -qE 'newTag: .* # x-release-please-version$'
  grep -A2 '^  - name: E2B_DASHBOARD_IMAGE$' kubernetes/kustomization.yaml | grep -qE 'newTag: v[0-9]+\.[0-9]+\.[0-9]+$'
}
