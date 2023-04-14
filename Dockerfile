FROM python:3.9


RUN ARCH="$(uname -m)"; \
    case "$ARCH" in \
    aarch64) pip install https://storage.googleapis.com/stashify-cdn/python/tikv_client-0.0.3-cp36-abi3-manylinux_2_31_aarch64.whl;; \
    x86_64) pip install https://storage.googleapis.com/stashify-cdn/python/tikv_client-0.0.3-cp36-abi3-manylinux_2_31_x86_64.whl;; \
    esac;

RUN pip install nucliadb-node-binding>=0.7.5

RUN mkdir -p /usr/src/app

RUN pip install Cython==0.29.24 pybind11 gunicorn uvicorn uvloop asyncpg


RUN set -eux; \
    dpkgArch="$(dpkg --print-architecture)"; \
    case "${dpkgArch##*-}" in \
    amd64) probeArch='amd64'; probeSha256='8d104fb997c9a5146a15a9c9f1fd45afa9d2dd995e185aeb96a19263fbd55b8a' ;; \
    arm64) probeArch='arm64'; probeSha256='6a74ac6eebb173987dd4a68fa99b74b2e1bdd3e0c7cf634c0d823595fbb28609' ;; \
    i386) probeArch='386'; probeSha256='eaed3339e273116d2c44a271d7245da1999b28a0c0bdf1d7b3aa75917712dc1a' ;; \
    *) echo >&2 "unsupported architecture: ${dpkgArch}"; exit 1 ;; \
    esac; \
    curl -L -o /bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.17/grpc_health_probe-linux-${probeArch}; \
    echo "${probeSha256} /bin/grpc_health_probe" | sha256sum -c -; \
    chmod +x /bin/grpc_health_probe

# Install entrypoint.sh dependencies
RUN apt-get update && apt-get install -y jq

# Copy source code
COPY VERSION /usr/src/app/VERSION
COPY nucliadb_utils /usr/src/app/nucliadb_utils
COPY nucliadb_telemetry /usr/src/app/nucliadb_telemetry
COPY nucliadb_protos /usr/src/app/nucliadb_protos
COPY nucliadb_models /usr/src/app/nucliadb_models
COPY nucliadb /usr/src/app/nucliadb

WORKDIR /usr/src/app

RUN pip install -r nucliadb/requirements-sources.txt
RUN pip install -e /usr/src/app/nucliadb

RUN mkdir -p /data

ENV NUA_ZONE=europe-1
ENV NUA_API_KEY=
ENV NUCLIA_PUBLIC_URL=https://{zone}.nuclia.cloud

ENV NUCLIADB_ENV=True

ENV DRIVER=LOCAL
ENV MAINDB=/data/maindb
ENV BLOB=/data/blobs
ENV NODE=/data/node

ENV HTTP=8080
ENV GRPC=8060
ENV TRAIN=8040

# HTTP
EXPOSE 8080/tcp
# GRPC
EXPOSE 8060/tcp
# GRPC - TRAIN
EXPOSE 8040/tcp

CMD ["nucliadb"]
