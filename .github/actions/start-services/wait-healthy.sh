#!/usr/bin/env bash
# Waits for a container that start-databases/start.sh kicked off to report
# healthy. The start ran detached from its step's output, logging under
# $RUNNER_TEMP/start-databases with its exit status written beside the log
# once it finished. A status other than 0 means the start itself failed (the
# pull gave up, docker run refused), so the wait fails at once instead of
# sitting out the health timeout; the start log is printed either way, so a
# retried pull or a refused run is readable from the job log.
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 <container> [timeout-seconds]"
  exit 1
fi

name="$1"
timeout="${2:-120}"
dir="${RUNNER_TEMP:-/tmp}/start-databases"

# start_log [open]: the last lines of the start's log, collapsed in the job
# log unless "open" asks for them in the clear.
start_log() {
  [ -f "$dir/$name.log" ] || return 0
  if [ "${1:-}" = open ]; then
    echo "--- start log of $name (last 50 lines)"
  else
    echo "::group::start log of $name (last 50 lines)"
  fi
  tail -n 50 "$dir/$name.log"
  [ "${1:-}" = open ] || echo "::endgroup::"
}

for ((i = 0; i < timeout; i += 2)); do
  if [ "$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null)" = "healthy" ]; then
    echo "$name is healthy!"
    start_log
    exit 0
  fi
  if [ -f "$dir/$name.status" ]; then
    read -r status < "$dir/$name.status"
    if [ "$status" != 0 ]; then
      echo "::error::$name failed to start (exit $status)"
      start_log open
      exit 1
    fi
  fi
  echo "Waiting for $name to be healthy..."
  sleep 2
done

echo "::error::$name did not become healthy within ${timeout}s"
start_log open
docker logs "$name" 2>&1 | tail -n 50 || true
exit 1
