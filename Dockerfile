FROM python:3.7

LABEL version="0.3.3"
LABEL maintainer="Alex Olieman <alex@olieman.net>"

RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update \
    && apt-get install -y \
       libsnappy-dev \
       zlib1g-dev \
       libbz2-dev \
       libgflags-dev \
       liblz4-dev \
       libzstd-dev \
    && apt-get clean

ENV BUILD_DIR=/build
WORKDIR ${BUILD_DIR}
COPY download_rocksdb.py ./
RUN ROCKS_VERSION=$(python download_rocksdb.py ${BUILD_DIR}) \
    && tar -xzf "${BUILD_DIR}/latest.tar.gz" \
    && cd "rocksdb-$ROCKS_VERSION" \
    && INSTALL_PATH=/usr make install-shared \
    && rm -rf ${BUILD_DIR}

ENV APP_DIR=/usr/src/app
WORKDIR ${APP_DIR}

# to avoid pip cache, this needs to be falsy
ENV PIP_NO_CACHE_DIR=false
COPY Pipfile ./
COPY Pipfile.lock ./
RUN pip install Cython pipenv
RUN pipenv install --system

COPY gunicorn_config.py ./

RUN mkdir same_thing
COPY ./same_thing ./same_thing
ENV PATH="${APP_DIR}:${PATH}"

RUN mkdir /dbdata
RUN mkdir /downloads
VOLUME [ "/usr/src/app", "/dbdata", "/downloads" ]

CMD [ "python", "-m", "same_thing.loader" ]
