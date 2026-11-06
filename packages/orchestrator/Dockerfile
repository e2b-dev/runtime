ARG GOLANG_VERSION=1.26.10
ARG DEBIAN_RUNTIME_TAG=bookworm-20260824-slim

# The orchestrator binary is built with CGO and dynamically links against
# glibc, so the build image's glibc must be <= the host's glibc (forward
# compatibility only). The host runs Ubuntu 24.04 (glibc 2.39), so bookworm
# (glibc 2.36) is safe; do NOT bump to trixie (glibc 2.41) without also
# upgrading the host, or the raw binary will fail with "GLIBC_2.4x not found".
ARG DEBIAN_VERSION=bookworm

FROM golang:${GOLANG_VERSION}-${DEBIAN_VERSION} AS builder
# Fall through to a retry on any module proxy error; see
# .github/actions/go-setup-cache/action.yml for the mechanism.
ENV GOPROXY=https://proxy.golang.org|https://proxy.golang.org:443|https://proxy.golang.org/|direct
ARG TARGETARCH

# Cached golang dependencies
WORKDIR /build/shared
COPY ./shared/go.mod ./shared/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build/clickhouse
COPY ./clickhouse/go.mod ./clickhouse/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build/orchestrator
COPY ./orchestrator/go.mod ./orchestrator/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy source code
WORKDIR /build

COPY ./shared/pkg ./shared/pkg
COPY ./clickhouse/pkg ./clickhouse/pkg

COPY ./orchestrator/pkg ./orchestrator/pkg
COPY ./orchestrator/cmd ./orchestrator/cmd
COPY ./orchestrator/main.go ./orchestrator/main.go
COPY ./orchestrator/Makefile ./orchestrator/Makefile

WORKDIR /build/orchestrator

ARG COMMIT_SHA
ARG VERSION
# Use Docker's TARGETARCH (set by --platform) so GOARCH matches the container platform.
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod make build-local COMMIT_SHA=${COMMIT_SHA} VERSION=${VERSION} BUILD_ARCH=${TARGETARCH}

FROM scratch AS artifacts

COPY --from=builder /build/orchestrator/bin/clean-nfs-cache .
COPY --from=builder /build/orchestrator/bin/orchestrator .

# No libc and no tools: the binary must stay static and exec nothing. The CA
# certificates are for the LaunchDarkly client.
FROM gcr.io/distroless/static-debian12@sha256:d75cdd72874d4790092fcb1b058493ecf6bb5bf2b2b897045b00ff01d91843f2 AS clean-nfs-cache

ARG COMMIT_SHA

LABEL org.opencontainers.image.title="clean-nfs-cache" \
      org.opencontainers.image.description="E2B privileged NFS cache cleanup CronJob" \
      org.opencontainers.image.source="https://github.com/e2b-dev/infra" \
      org.opencontainers.image.revision="${COMMIT_SHA}"

COPY --from=builder /build/orchestrator/bin/clean-nfs-cache /clean-nfs-cache

USER 0
ENTRYPOINT ["/clean-nfs-cache"]

FROM debian:${DEBIAN_RUNTIME_TAG} AS runtime

ARG COMMIT_SHA

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        bash \
        util-linux \
        iproute2 \
        iptables \
        rsync \
        e2fsprogs \
        systemd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/orchestrator/bin/orchestrator /usr/local/bin/orchestrator

ENTRYPOINT ["/usr/local/bin/orchestrator"]
