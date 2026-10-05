FROM alpine:3.24
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-09-11
RUN apk upgrade --no-cache && apk add --no-cache bash util-linux curl jq coreutils ca-certificates
# The one-shot host scripts and the orchestrator launcher travel inside the
# image, so compose.yaml needs no repository mount.
COPY scripts/preflight.sh scripts/host-setup.sh scripts/fetch-artifacts.sh \
     scripts/host-teardown.sh scripts/orchestrator-launch.sh /opt/e2b/scripts/
