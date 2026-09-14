ARG GOLANG_VERSION=1.26.8
ARG ALPINE_VERSION=3.24

# Build stage
FROM golang:${GOLANG_VERSION}-alpine${ALPINE_VERSION} AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the plugin
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /nomad-nodepool-apm .

# Final stage - minimal image, but shared with builder
FROM alpine:${ALPINE_VERSION}
# Bump this date to force a rebuild from here down, so the upgrade below
# resolves against the current Alpine package index. Nothing reads the value.
ENV LAST_FORCED_UPDATE=2026-09-11

RUN apk upgrade --no-cache

COPY --from=builder /nomad-nodepool-apm /nomad-nodepool-apm

# The plugin binary will be extracted from this image
# to be used with the Nomad Autoscaler
ENTRYPOINT ["/nomad-nodepool-apm"]
