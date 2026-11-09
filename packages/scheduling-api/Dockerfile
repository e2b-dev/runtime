ARG GOLANG_VERSION=1.27.3
ARG ALPINE_VERSION=3.24

# The builder runs on the build platform and cross-compiles for the target
# (TARGETARCH -> GOARCH), so an arm64 image is not a QEMU-emulated Go build.
FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
# Fall through to a retry on any module proxy error; see
# .github/actions/go-setup-cache/action.yml for the mechanism.
ENV GOPROXY=https://proxy.golang.org|https://proxy.golang.org:443|https://proxy.golang.org/|direct
ARG TARGETARCH

WORKDIR /build/shared
COPY ./shared/go.mod ./shared/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build/scheduling-api
COPY ./scheduling-api/go.mod ./scheduling-api/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

WORKDIR /build
COPY ./shared ./shared
COPY ./scheduling-api ./scheduling-api

WORKDIR /build/scheduling-api
# An empty VERSION keeps the release-please default compiled into main.go.
ARG COMMIT_SHA
ARG VERSION
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 GOARCH=${TARGETARCH} go build -ldflags "-X=main.commitSHA=${COMMIT_SHA} ${VERSION:+-X=main.version=${VERSION}}" -o /build/bin/scheduling-api .
RUN --mount=type=cache,target=/root/.cache/go-build --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 GOARCH=${TARGETARCH} go build -o /build/bin/scheduling-api-migrate ./cmd/migrate

FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-10-20
RUN apk upgrade --no-cache && apk add --no-cache ca-certificates
COPY --from=builder /build/bin/scheduling-api /usr/local/bin/scheduling-api
COPY --from=builder /build/bin/scheduling-api-migrate /usr/local/bin/scheduling-api-migrate
USER nobody
ENTRYPOINT ["/usr/local/bin/scheduling-api"]
