FROM python:3.7-slim-stretch AS build-stage

RUN python3 -m venv /venv
ENV PATH=/venv/bin:${PATH}

RUN mkdir /app /app/vendor
WORKDIR /app

RUN apt-get update -q \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -q -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -q -y --no-install-recommends \
        build-essential \
        cmake \
        git-core \
        pkg-config \
        curl \
        libssl-dev \
        gnupg2 \
        dirmngr \
        libgdal20 \
        libgdal-dev \
        sqlite3 \
        gdal-bin \
        libsqlite3-mod-spatialite \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /venv/bin/tini
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini.asc /tmp/tini.asc
RUN mkdir ~/.gnupg \
    && echo "disable-ipv6" >> ~/.gnupg/dirmngr.conf \
    && gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys 595E85A6B1B4779EA4DAAEC70B588DFF0527A9B7 \
    && gpg --batch --verify /tmp/tini.asc /venv/bin/tini \
    && chmod +x /venv/bin/tini

# Build LibGit2
ENV LIBGIT2=/venv
RUN git clone --branch merge-analysis-bare-repo-5017 --single-branch https://github.com/rcoup/libgit2.git /app/vendor/libgit2 \
    && cd /app/vendor/libgit2 \
    && cmake . -DCMAKE_INSTALL_PREFIX=${LIBGIT2} \
    && make \
    && make install

# build pygit2
RUN git clone --branch better-tree-nav --single-branch https://github.com/rcoup/pygit2.git /app/vendor/pygit2 \
    && export LDFLAGS="-Wl,-rpath='${LIBGIT2}/lib',--enable-new-dtags $LDFLAGS" \
    && cd /app/vendor/pygit2 \
    && pip install .

RUN pip install pygdal=="$(gdal-config --version).*"

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

RUN pip install /app
RUN rm -r /venv/include /venv/share

###############################################################################

FROM python:3.7-slim-stretch AS run-stage

# Try to record a Python traceback on crashes
ENV PYTHONFAULTHANDLER=true
ENV PATH=/venv/bin:${PATH}

RUN useradd --create-home snowdrop \
    && mkdir /data

RUN apt-get update -q \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -q -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -q -y --no-install-recommends \
        git-core \
        sqlite3 \
        libgdal20 \
        gdal-bin \
        libsqlite3-mod-spatialite \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build-stage --chown=snowdrop:snowdrop /venv /venv

USER snowdrop
WORKDIR /data

# smoke test
RUN kxgit --version

ENTRYPOINT ["/venv/bin/tini", "--"]
CMD ["/venv/bin/kxgit"]
