ARG GOLANG_VERSION=1.26.8
ARG ALPINE_VERSION=3.24

# Builder stage
FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG TARGETARCH

RUN apk add --no-cache git
# A cross-compiled `go install` lands in /go/bin/linux_<arch>/, a native one in /go/bin/.
RUN --mount=type=cache,target=/root/.cache/go-build GOARCH=${TARGETARCH} go install github.com/pressly/goose/v3/cmd/goose@v3.28.0 \
    && install -D "$(find /go/bin -type f -name goose)" /out/goose

# Final stage
FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-09-11

RUN apk upgrade --no-cache && apk add --no-cache bash ca-certificates

COPY --from=builder /out/goose /usr/local/bin/goose

WORKDIR /app
COPY migrations/ ./migrations/

ENV GOOSE_DRIVER=clickhouse
ENTRYPOINT ["goose", "-table", "_migrations", "-dir", "migrations", "up"]
