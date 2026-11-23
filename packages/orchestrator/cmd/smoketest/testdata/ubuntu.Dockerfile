FROM ubuntu:22.04
ARG PACKAGES
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends $PACKAGES \
    && rm -rf /var/lib/apt/lists/* /etc/apt/sources.list /etc/apt/sources.list.d/*
