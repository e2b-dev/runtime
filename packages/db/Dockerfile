ARG GOLANG_VERSION=1.26.8
ARG ALPINE_VERSION=3.24

# Builder stage
FROM --platform=$BUILDPLATFORM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder
ARG TARGETARCH

# Cached golang dependencies
WORKDIR /build/shared
COPY ./shared/go.mod ./shared/go.sum ./
RUN go mod download

WORKDIR /build/db
COPY ./db/go.mod ./db/go.sum ./
RUN go mod download

# Copy source code
WORKDIR /build

COPY ./shared/pkg ./shared/pkg

COPY ./db/scripts/migrator.go ./db/scripts/migrator.go
COPY ./db/migrations/ ./db/migrations

WORKDIR /build/db

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOARCH=${TARGETARCH} go build -o ./bin/migrator ./scripts/migrator.go
RUN chmod +x ./bin/migrator

FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-09-11

RUN apk upgrade --no-cache

COPY --from=builder /build/db/bin/migrator .
COPY --from=builder  /build/db/migrations ./migrations

ENTRYPOINT ["./migrator"]
