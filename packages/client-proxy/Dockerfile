ARG GOLANG_VERSION=1.26.8
ARG ALPINE_VERSION=3.24

FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG TARGETARCH

RUN apk add --no-cache make

# Cached golang dependencies
WORKDIR /build/shared
COPY ./shared/go.mod ./shared/go.sum ./
RUN go mod download

WORKDIR /build/clickhouse
COPY ./clickhouse/go.mod ./clickhouse/go.sum ./
RUN go mod download

WORKDIR /build/client-proxy
COPY ./client-proxy/go.mod ./client-proxy/go.sum ./
RUN go mod download

# Copy source code
WORKDIR /build

COPY ./shared/pkg ./shared/pkg

COPY ./clickhouse/pkg ./clickhouse/pkg

COPY ./client-proxy/internal ./client-proxy/internal
COPY ./client-proxy/main.go ./client-proxy/main.go
COPY ./client-proxy/Makefile ./client-proxy/Makefile

WORKDIR /build/client-proxy

ARG COMMIT_SHA
ARG VERSION
# Use Docker's TARGETARCH so GOARCH matches the container platform.
RUN --mount=type=cache,target=/root/.cache/go-build make build COMMIT_SHA=${COMMIT_SHA} VERSION=${VERSION} BUILD_ARCH=${TARGETARCH}
RUN chmod +x /build/client-proxy/bin/client-proxy

FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-09-11

RUN apk upgrade --no-cache

COPY --from=builder /build/client-proxy/bin/client-proxy .

ENTRYPOINT ["./client-proxy"]
