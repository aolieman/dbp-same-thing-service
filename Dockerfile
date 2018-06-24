FROM python:3

LABEL version="0.1.2"
LABEL maintainer="Alex Olieman <alex@olieman.net>"

RUN echo "deb http://ftp.debian.org/debian stretch-backports main" >> /etc/apt/sources.list
RUN DEBIAN_FRONTEND=noninteractive \
    apt-get update \
    && apt-get install -y \
       libsnappy-dev \
       zlib1g-dev \
       libbz2-dev \
       libgflags-dev \
       liblz4-dev \
    && apt-get -t stretch-backports install -y "libzstd-dev" \
    && apt-get clean

RUN mkdir /build \
    && cd /build \
    && git clone https://github.com/facebook/rocksdb.git \
    && cd rocksdb \
    && INSTALL_PATH=/usr make install-shared \
    && rm -rf /build

WORKDIR /usr/src/app
COPY gunicorn_config.py ./

# to avoid pip cache, this needs to be falsy
ENV PIP_NO_CACHE_DIR=false
COPY Pipfile ./
COPY Pipfile.lock ./
RUN pip install Cython pipenv
RUN pipenv install --system

RUN mkdir same_thing
COPY ./same_thing ./same_thing
RUN mkdir /dbdata
RUN mkdir /downloads
VOLUME [ "/usr/src/app", "/dbdata", "/downloads" ]

CMD [ "python", "-m", "same_thing.loader" ]
