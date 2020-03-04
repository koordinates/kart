# syntax = docker/dockerfile:experimental
# Need to build this using: `DOCKER_BUILDKIT=1 docker build ...`
FROM python:3.7-slim-stretch AS build-stage1

RUN rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,id=aptc1,target=/var/cache/apt --mount=type=cache,id=aptv1,target=/var/lib/apt \
    apt-get update -q \
    && DEBIAN_FRONTEND=noninteractive apt-get install -q -y --no-install-recommends \
        dirmngr \
        gnupg2

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/local/bin/tini
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini.asc /usr/local/share/tini.asc
RUN mkdir ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && gpg --batch --keyserver hkp://pgp.key-server.io --recv-keys 595E85A6B1B4779EA4DAAEC70B588DFF0527A9B7 \
    && gpg --batch --verify /usr/local/share/tini.asc /usr/local/bin/tini \
    && chmod +x /usr/local/bin/tini

# # ###############################################################################

FROM python:3.7-slim-stretch AS build-stage2

WORKDIR /src
ENV VIRTUAL_ENV=/venv

RUN python3 -m venv /venv \
    && /venv/bin/python -m pip install --upgrade pip
ENV PATH=/venv/bin:$PATH

COPY vendor/wheelhouse/mod_spatialite.so /venv/lib
COPY vendor/env/share/gdal/ /venv/share/gdal/
COPY vendor/env/share/proj/ /venv/share/proj/

COPY requirements.txt /src/
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-deps -r requirements.txt

COPY vendor/wheelhouse/GDAL-*-cp37-cp37m-manylinux2010_x86_64.whl /src
RUN pip install --no-deps /src/GDAL*.whl
COPY vendor/wheelhouse/pygit2-*-cp37-cp37m-manylinux2010_x86_64.whl /src
RUN pip install --no-deps /src/pygit2*.whl

COPY setup.py /src/
COPY sno/ /src/sno/
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-deps . \
    && sno --version


# ###############################################################################

FROM python:3.7-slim-stretch AS run-stage

# Try to record a Python traceback on crashes
ENV PYTHONFAULTHANDLER=true

RUN useradd --create-home sno \
    && mkdir /data \
    && rm -f /etc/apt/apt.conf.d/docker-clean \
    && echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache

COPY --from=build-stage1 /usr/local/bin/tini /usr/local/bin/tini

COPY requirements/run.apt /etc/run.apt
RUN --mount=type=cache,id=aptcr,target=/var/cache/apt --mount=type=cache,id=aptvr,target=/var/lib/apt \
    apt-get update -q \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -q -y \
    && /bin/bash -c "DEBIAN_FRONTEND=noninteractive xargs -a <(awk '/^\s*[^#]/' /etc/run.apt) -r -- apt-get install -q -y --no-install-recommends"

COPY --from=build-stage2 /venv/ /venv/

RUN ln -s /venv/bin/sno /usr/local/bin/sno \
    && sno --version

USER sno
WORKDIR /data

ENTRYPOINT ["/usr/local/bin/tini", "--"]
CMD ["sno"]
