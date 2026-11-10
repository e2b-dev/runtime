#!/bin/bash

# Pull a Docker image with retries. Anonymous Docker Hub pulls from shared
# runners intermittently fail with "unauthorized: authentication required"
# or "toomanyrequests" (Hub rate limiting); an implicit pull inside docker
# run then aborts the whole job. Docker Hub images are therefore pulled
# from mirror.gcr.io, Google's public cache of Docker Hub, and tagged with
# the name the caller uses; Docker Hub itself is only the fallback.
# Near-instant when the image is already present, so it is safe to call
# both as a background warm-up and as a synchronous guarantee.

set -uo pipefail

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <image>"
    exit 1
fi

IMAGE="$1"
HUB_MIRROR="mirror.gcr.io"

# A Docker Hub image has no registry host: its first path component is not a
# hostname (no "." or ":", not "localhost"), or it has no "/" at all.
is_hub_image() {
    local first="${1%%/*}"
    [ "$first" = "$1" ] && return 0
    case "$first" in
        *.* | *:* | localhost) return 1 ;;
    esac
    return 0
}

pull() {
    if is_hub_image "$IMAGE"; then
        if docker pull --quiet "$HUB_MIRROR/$IMAGE" && docker tag "$HUB_MIRROR/$IMAGE" "$IMAGE"; then
            return 0
        fi
        echo "pull of $IMAGE from $HUB_MIRROR failed, trying Docker Hub"
    fi
    docker pull --quiet "$IMAGE"
}

for attempt in 1 2 3 4 5; do
    pull && exit 0
    if [ "$attempt" = 5 ]; then
        echo "::error::giving up on pulling $IMAGE"
        exit 1
    fi
    echo "pull of $IMAGE failed (attempt $attempt), retrying in $((attempt * 10))s..."
    sleep $((attempt * 10))
done
