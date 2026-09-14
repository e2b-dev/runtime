ARG GOLANG_VERSION=1.26.8
ARG ALPINE_VERSION=3.24

FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG TARGETARCH

RUN apk add --no-cache make

# Copy all module dependency files at once for better Docker layer caching
WORKDIR /build
COPY ./shared/go.mod ./shared/go.sum ./shared/
COPY ./clickhouse/go.mod ./clickhouse/go.sum ./clickhouse/
COPY ./db/go.mod ./db/go.sum ./db/
COPY ./auth/go.mod ./auth/go.sum ./auth/
COPY ./api/go.mod ./api/go.sum ./api/

# Download all module dependencies
RUN cd /build/shared && go mod download && \
    cd /build/clickhouse && go mod download && \
    cd /build/db && go mod download && \
    cd /build/auth && go mod download && \
    cd /build/api && go mod download

# Copy source code
WORKDIR /build

COPY ./auth/pkg ./auth/pkg
COPY ./clickhouse/pkg ./clickhouse/pkg
COPY ./shared/pkg ./shared/pkg
COPY ./db/client ./db/client
COPY ./db/queries ./db/queries
COPY ./db/pkg ./db/pkg

COPY ./api/internal ./api/internal
COPY ./api/main.go ./api/main.go
COPY ./api/Makefile ./api/Makefile

WORKDIR /build/api

ARG COMMIT_SHA
ARG VERSION
ARG EXPECTED_MIGRATION_TIMESTAMP
RUN --mount=type=cache,target=/root/.cache/go-build GOARCH=${TARGETARCH} make build COMMIT_SHA=${COMMIT_SHA} VERSION=${VERSION} EXPECTED_MIGRATION_TIMESTAMP=${EXPECTED_MIGRATION_TIMESTAMP}

RUN chmod +x /build/api/bin/api

FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-09-11

RUN apk upgrade --no-cache

COPY --from=builder /build/api/bin/api .

# Set Gin server to the production mode
ENV GIN_MODE=release

ENTRYPOINT [ "./api"]
