ARG GOLANG_VERSION=1.27.3
ARG ALPINE_VERSION=3.24

# Builder stage
FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
# Fall through to a retry on any module proxy error; see
# .github/actions/go-setup-cache/action.yml for the mechanism.
ENV GOPROXY=https://proxy.golang.org|https://proxy.golang.org:443|https://proxy.golang.org/|direct
ARG TARGETARCH

RUN apk add --no-cache git
# A cross-compiled `go install` lands in /go/bin/linux_<arch>/, a native one in /go/bin/.
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod GOARCH=${TARGETARCH} go install github.com/pressly/goose/v3/cmd/goose@v3.28.0 \
    && install -D "$(find /go/bin -type f -name goose)" /out/goose

# Final stage
FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-10-20

RUN apk upgrade --no-cache && apk add --no-cache bash ca-certificates

COPY --from=builder /out/goose /usr/local/bin/goose

WORKDIR /app
COPY migrations/ ./migrations/

ENV GOOSE_DRIVER=clickhouse
ENTRYPOINT ["goose", "-table", "_migrations", "-dir", "migrations", "up"]
