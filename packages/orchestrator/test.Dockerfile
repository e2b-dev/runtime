ARG GOLANG_VERSION=1.26.8

# The orchestrator binary is built with CGO and dynamically links against
# glibc, so the build image's glibc must be <= the host's glibc (forward
# compatibility only). The host runs Ubuntu 24.04 (glibc 2.39), so bookworm
# (glibc 2.36) is safe; do NOT bump to trixie (glibc 2.41) without also
# upgrading the host, or the binary will fail to start with
# "GLIBC_2.4x not found" errors.
ARG DEBIAN_VERSION=bookworm

# Dockerfile for running orchestrator golang tests
FROM golang:${GOLANG_VERSION}-${DEBIAN_VERSION} AS base

WORKDIR /build/shared

# Copy shared package dependencies
COPY .shared/go.mod .shared/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY .shared/pkg pkg

WORKDIR /build/clickhouse

# Copy clickhouse package dependencies
COPY .clickhouse/go.mod .clickhouse/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY .clickhouse/pkg pkg

WORKDIR /build/orchestrator

# Copy orchestrator dependencies
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy source code
COPY main.go Makefile ./
COPY scripts scripts
COPY pkg pkg
COPY cmd cmd

FROM base AS runner

RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod make test
