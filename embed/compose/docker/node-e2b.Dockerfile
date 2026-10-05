FROM node:22.23.2-alpine3.24
# The pinned tag freezes the Alpine packages at whatever the base image was
# built with; pull the release branch's current ones in. Bump the date to
# force a rebuild from here down, so that upgrade resolves against the
# current package index.
ENV LAST_FORCED_UPDATE=2026-09-11
RUN apk upgrade --no-cache
WORKDIR /app
COPY scripts/node/package.json scripts/node/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY scripts/node/build-base-template.mjs scripts/node/smoke.mjs ./
