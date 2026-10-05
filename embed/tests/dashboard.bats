#!/usr/bin/env bats

# The dashboard is two services on the host network beside the api: the web
# UI on 3001 and its backend dashboard-api on 3010. These tests hold the
# contract the published dashboard image imposes and the guides describe:
# the runtime URL variables are the image's names, the SDK alias
# E2B_SANDBOX_URL is never set (the image refuses to start on it), the cookie
# is not marked Secure (plain http), and the sandbox URL the browser uses
# comes from the E2B_DASHBOARD_HOST knob, localhost by default.
#
# Rendering needs Docker with the compose plugin and jq, as secrets.bats does.
# The knob and the api secrets are unset for the render so a value in the
# runner's shell is not what these assertions see.

setup() {
  cd "$BATS_TEST_DIRNAME/.." || return 1
  RENDERED_JSON="$(env -u E2B_DASHBOARD_HOST -u ADMIN_TOKEN -u SANDBOX_ACCESS_TOKEN_HASH_SEED \
    docker compose --project-directory compose config --format json)"
}

env_pin() { sed -n "s|^$1=\([^ #]*\).*|\1|p" compose/.env; }

svc() { echo "$RENDERED_JSON" | jq -r ".services[\"$1\"]$2"; }

@test "both dashboard services exist, on the host network, on their pinned images" {
  [ "$(svc dashboard-api .image)" = "$(env_pin E2B_DASHBOARD_API_IMAGE)" ]
  [ "$(svc dashboard .image)" = "$(env_pin E2B_DASHBOARD_IMAGE)" ]
  [ "$(svc dashboard-api .network_mode)" = "host" ]
  [ "$(svc dashboard .network_mode)" = "host" ]
}

@test "the dashboard carries exactly the image's runtime variables" {
  keys="$(svc dashboard '.environment | keys | sort | join(" ")')"
  [ "$keys" = "DASHBOARD_COOKIE_SECURE E2B_DASHBOARD_API_URL E2B_INFRA_API_URL HOSTNAME PORT PUBLIC_E2B_DOMAIN PUBLIC_SANDBOX_URL" ] || {
    echo "dashboard env keys are: $keys" >&2; return 1; }
  [ "$(svc dashboard .environment.PORT)" = "3001" ]
  [ "$(svc dashboard .environment.DASHBOARD_COOKIE_SECURE)" = "false" ]
  [ "$(svc dashboard .environment.E2B_INFRA_API_URL)" = "http://127.0.0.1:3000" ]
  [ "$(svc dashboard .environment.E2B_DASHBOARD_API_URL)" = "http://127.0.0.1:3010" ]
  [ "$(svc dashboard .environment.PUBLIC_E2B_DOMAIN)" = "localhost" ]
}

@test "the browser's sandbox URL follows E2B_DASHBOARD_HOST and defaults to localhost" {
  [ "$(svc dashboard .environment.PUBLIC_SANDBOX_URL)" = "http://localhost:3002" ]
  run bash -c 'E2B_DASHBOARD_HOST=203.0.113.7 docker compose --project-directory compose config --format json | jq -r ".services.dashboard.environment.PUBLIC_SANDBOX_URL"'
  [ "$output" = "http://203.0.113.7:3002" ]
}

@test "the SDK alias and the build-time names are never set on the dashboard" {
  for key in E2B_SANDBOX_URL NEXT_PUBLIC_E2B_DOMAIN NEXT_PUBLIC_INFRA_API_URL NEXT_PUBLIC_DASHBOARD_API_URL NEXT_PUBLIC_E2B_SANDBOX_URL; do
    [ "$(svc dashboard ".environment | has(\"$key\")")" = "false" ] || { echo "dashboard sets $key" >&2; return 1; }
  done
}

@test "dashboard-api reaches the stores the api's way and takes the admin token from the seed-state volume" {
  [ "$(svc dashboard-api .environment.PORT)" = "3010" ]
  [ "$(svc dashboard-api .environment.POSTGRES_CONNECTION_STRING)" = "$(svc api .environment.POSTGRES_CONNECTION_STRING)" ]
  [ "$(svc dashboard-api .environment.REDIS_URL)" = "$(svc api .environment.REDIS_URL)" ]
  [ "$(svc dashboard-api .environment.CLICKHOUSE_CONNECTION_STRING)" = "$(svc api .environment.CLICKHOUSE_CONNECTION_STRING)" ]
  [ "$(svc dashboard-api '.environment.ADMIN_TOKEN | type, length' | tr '\n' ' ')" = "string 0 " ]
  [ "$(svc dashboard-api '.volumes[] | select(.target == "/run/e2b") | "\(.source) \(.read_only)"')" = "seed-state true" ]
  for key in ORY_SDK_URL ORY_PROJECT_API_TOKEN ADMIN_AUTH_PROVIDER_CONFIG; do
    [ "$(svc dashboard-api ".environment | has(\"$key\")")" = "false" ] || { echo "dashboard-api sets $key" >&2; return 1; }
  done
}

@test "start order: dashboard-api after the migrator and the secrets, the dashboard after both apis" {
  [ "$(svc dashboard-api '.depends_on["db-migrator"].condition')" = "service_completed_successfully" ]
  [ "$(svc dashboard-api '.depends_on["api-secrets"].condition')" = "service_completed_successfully" ]
  [ "$(svc dashboard-api '.depends_on.postgres.condition')" = "service_healthy" ]
  [ "$(svc dashboard '.depends_on.api.condition')" = "service_healthy" ]
  [ "$(svc dashboard '.depends_on["dashboard-api"].condition')" = "service_healthy" ]
  [ "$(svc ready '.depends_on.dashboard.condition')" = "service_healthy" ]
}

@test "the healthchecks probe the ports the guides name" {
  [[ "$(svc dashboard-api '.healthcheck.test | join(" ")')" == *"http://127.0.0.1:3010/health"* ]]
  test_cmd="$(svc dashboard '.healthcheck.test | join(" ")')"
  [[ "$test_cmd" == "CMD node -e "* ]]
  [[ "$test_cmd" == *"http://127.0.0.1:3001/api/health"* ]]
}

@test "ready writes the dashboard URL beside the SDK variables and says what to do with it" {
  script="$(echo "$RENDERED_JSON" | jq -r '.services.ready.command[2]' | sed 's/\$\$/$/g')"
  [[ "$script" == *'export E2B_DASHBOARD_URL=http://%s:3001'* ]]
  [[ "$script" == *'Open E2B_DASHBOARD_URL in a browser and paste E2B_API_KEY into its key form.'* ]]
}
