#!/bin/bash

# Default timeout, override with STARTUP_TIMEOUT env
TIMEOUT=${STARTUP_TIMEOUT:-30}

if [ "$#" -ne 5 ]; then
    echo "Usage: $0 <name> <make_path> <make_command> <log_file> <health_url>"
    exit 1
fi

NAME="$1"
MAKE_PATH="$2"
MAKE_COMMAND="$3"
LOG_FILE="$4"
HEALTH_URL="$5"

# A Go panic prints its message before the goroutine dump, so a failed start prints the whole log.
fail() {
    echo "::error::$1"
    echo "--- $LOG_FILE"
    cat "$LOG_FILE"
    exit 1
}

echo "Starting $NAME..."
# The runner waits 5 s at step end while a background child holds the step's stdout.
make -C "$MAKE_PATH" "$MAKE_COMMAND" </dev/null >"$LOG_FILE" 2>&1 &
pid=$!

echo "Waiting for $NAME to become healthy at $HEALTH_URL (timeout: $TIMEOUT seconds)..."
for ((i = 0; i < TIMEOUT * 10; i++)); do
    if curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL" | grep -q 200; then
        echo "$NAME is healthy and running."
        echo "::group::$NAME log so far ($LOG_FILE)"
        cat "$LOG_FILE"
        echo "::endgroup::"
        exit 0
    fi
    # make exiting 0 may have left a detached service starting, so only a failed exit ends the wait.
    if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
        status=0
        wait "$pid" || status=$?
        pid=
        [ "$status" -eq 0 ] || fail "$NAME exited with status $status before becoming healthy."
    fi
    sleep 0.1
done

fail "$NAME failed to become healthy in time."
