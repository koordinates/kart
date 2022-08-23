FROM ubuntu:bionic

RUN DEBIAN_FRONTEND=noninteractive apt-get update -q -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ruby \
        ruby-dev \
        gcc \
        libc6-dev \
        make \
        ca-certificates \
        libffi-dev \
        ruby-ffi \
        rpm \
        git \
    && gem install fpm -v 1.10.0 \
    && mkdir /src

WORKDIR /src
