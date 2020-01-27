FROM python:3.7-slim-stretch AS build-stage

RUN apt-get update -q \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -q -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -q -y --no-install-recommends \
        dirmngr \
        gnupg2 \
        make

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/local/bin/tini
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini.asc /usr/local/share/tini.asc
RUN mkdir ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 595E85A6B1B4779EA4DAAEC70B588DFF0527A9B7 \
    && gpg --batch --verify /usr/local/share/tini.asc /usr/local/bin/tini \
    && chmod +x /usr/local/bin/tini

COPY requirements/*.apt /reqs/
RUN /bin/bash -c "DEBIAN_FRONTEND=noninteractive xargs -a <(awk '/^\s*[^#]/' /reqs/*.apt) -r -- apt-get install -q -y --no-install-recommends" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /src/

ARG PIP_INDEX_URL=
ARG PIP_TRUSTED_HOST=

RUN cd /src \
    &&  VIRTUAL_ENV=/venv \
        VENDOR_GIT_CLONE_ARGS=--depth=1 \
        PY_PIP_ARGS= \
        make py-app \
    && /venv/bin/sno --version

# ###############################################################################

FROM python:3.7-slim-stretch AS run-stage

# Try to record a Python traceback on crashes
ENV PYTHONFAULTHANDLER=true

RUN useradd --create-home sno \
    && mkdir /data
COPY --from=build-stage /usr/local/bin/tini /usr/local/bin/tini

COPY requirements/run.apt /etc/run.apt
RUN apt-get update -q \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -q -y \
    && /bin/bash -c "DEBIAN_FRONTEND=noninteractive xargs -a <(awk '/^\s*[^#]/' /etc/run.apt) -r -- apt-get install -q -y --no-install-recommends" \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-stage /venv /venv

RUN ln -s /venv/bin/sno /usr/local/bin/sno \
    && sno --version

USER sno
WORKDIR /data

ENTRYPOINT ["/usr/local/bin/tini", "--"]
CMD ["sno"]
