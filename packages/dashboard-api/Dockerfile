ARG GOLANG_VERSION=1.27.3
ARG ALPINE_VERSION=3.24

# The builder runs on the build platform and cross-compiles for the target
# (TARGETARCH -> GOARCH), so an arm64 image is not a QEMU-emulated Go build.
FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
# Fall through to a retry on any module proxy error; see
# .github/actions/go-setup-cache/action.yml for the mechanism.
ENV GOPROXY=https://proxy.golang.org|https://proxy.golang.org:443|https://proxy.golang.org/|direct
ARG TARGETARCH

RUN apk add --no-cache make

# Cached golang dependencies
WORKDIR /build/shared
COPY ./shared/go.mod ./shared/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build/clickhouse
COPY ./clickhouse/go.mod ./clickhouse/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build/db
COPY ./db/go.mod ./db/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build/auth
COPY ./auth/go.mod ./auth/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build/dashboard-api
COPY ./dashboard-api/go.mod ./dashboard-api/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy source code
WORKDIR /build

COPY . .

WORKDIR /build/dashboard-api

ARG COMMIT_SHA
ARG VERSION
ARG EXPECTED_MIGRATION_TIMESTAMP
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod GOARCH=${TARGETARCH} make build COMMIT_SHA=${COMMIT_SHA} VERSION=${VERSION} EXPECTED_MIGRATION_TIMESTAMP=${EXPECTED_MIGRATION_TIMESTAMP}
RUN chmod +x /build/dashboard-api/bin/dashboard-api

FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-10-20

RUN apk upgrade --no-cache

COPY --from=builder /build/dashboard-api/bin/dashboard-api .

ENTRYPOINT ["./dashboard-api"]
