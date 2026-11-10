#!/usr/bin/env bash
# Kicks off the postgres, redis and clickhouse containers in the background so
# their image pulls (with rate-limit retries, see scripts/pull-retry.sh) and
# boots overlap with the package builds that follow. The runner stops reading
# a step's output once the step's shell exits, so a start that still wrote to
# it would be invisible; each start therefore runs detached from this step's
# stdin and output, writes to its own log under $RUNNER_TEMP/start-databases
# and records its exit status beside it. start-services/wait-healthy.sh reads
# both: a non-zero status fails the wait at once instead of after the health
# timeout, and the log says why.
set -euo pipefail

dir="${RUNNER_TEMP:-/tmp}/start-databases"
mkdir -p "$dir"

# Errexit is off inside the started function (see in_background), so a start
# chains its steps with && to stop at the first failure.
start_postgres() {
  ./scripts/pull-retry.sh postgres:18 &&
    docker run -d --name postgres \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=local \
      -e POSTGRES_DB=mydatabase \
      -p 5432:5432 \
      --health-cmd="pg_isready -U postgres" \
      --health-interval=5s \
      --health-timeout=2s \
      --health-retries=5 \
      postgres:18
}

start_redis() {
  ./scripts/pull-retry.sh redis:8 &&
    docker run -d --name redis \
      -p 6379:6379 \
      --health-cmd="redis-cli ping" \
      --health-interval=5s \
      --health-timeout=2s \
      --health-retries=5 \
      redis:8
}

# The Makefile runs the container in the foreground, so this start keeps
# streaming the server's log and records its status only when the container
# exits: at once when it fails to boot, never while the job is healthy.
start_clickhouse() {
  ./scripts/pull-retry.sh clickhouse/clickhouse-server:25.8.30.16 &&
    make -C packages/clickhouse run
}

# Warm-up only: nothing waits on its status. The guarantee lives in
# start-services, which re-runs pull-retry.sh for this image synchronously
# before starting the otel collector. The tag comes from the Makefile that
# runs the image, so a version bump there can't strand this pull.
start_otel_collector() {
  local image
  image=$(grep -oE 'otel/opentelemetry-collector-contrib:[0-9.]+' packages/otel-collector/Makefile | head -1) &&
    ./scripts/pull-retry.sh "$image"
}

# in_background <name> <function>: the whole subshell is redirected, so no
# process it leaves behind holds this step's output open. The status is
# written last and moved into place, so when the file exists it is complete
# and the log is final.
in_background() {
  local name="$1" start="$2"
  (
    set +e
    "$start"
    echo $? > "$dir/$name.status.tmp" && mv "$dir/$name.status.tmp" "$dir/$name.status"
  ) > "$dir/$name.log" 2>&1 < /dev/null &
}

in_background postgres start_postgres
in_background redis start_redis
in_background clickhouse start_clickhouse
in_background otel-collector start_otel_collector
