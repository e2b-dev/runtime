ARG GOLANG_VERSION=1.26.8

# The orchestrator binary is built with CGO and dynamically links against
# glibc, so the build image's glibc must be <= the host's glibc (forward
# compatibility only). The host runs Ubuntu 24.04 (glibc 2.39), so bookworm
# (glibc 2.36) is safe; do NOT bump to trixie (glibc 2.41) without also
# upgrading the host, or the binary will fail to start with
# "GLIBC_2.4x not found" errors.
ARG DEBIAN_VERSION=bookworm

FROM golang:${GOLANG_VERSION}-${DEBIAN_VERSION} AS builder
ARG TARGETARCH

# Cached golang dependencies
WORKDIR /build/shared
COPY ./shared/go.mod ./shared/go.sum ./
RUN go mod download

WORKDIR /build/clickhouse
COPY ./clickhouse/go.mod ./clickhouse/go.sum ./
RUN go mod download

WORKDIR /build/orchestrator
COPY ./orchestrator/go.mod ./orchestrator/go.sum ./
RUN go mod download

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
RUN --mount=type=cache,target=/root/.cache/go-build make build-local COMMIT_SHA=${COMMIT_SHA} VERSION=${VERSION} BUILD_ARCH=${TARGETARCH}

FROM scratch

COPY --from=builder /build/orchestrator/bin/clean-nfs-cache .
COPY --from=builder /build/orchestrator/bin/orchestrator .
