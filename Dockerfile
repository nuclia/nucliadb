FROM python:3.9


RUN ARCH="$(uname -m)"; \
    case "$ARCH" in \
        aarch64) pip install https://storage.googleapis.com/stashify-cdn/python/tikv_client-0.0.3-cp36-abi3-manylinux_2_31_aarch64.whl;; \
        x86_64) pip install https://storage.googleapis.com/stashify-cdn/python/tikv_client-0.0.3-cp36-abi3-manylinux_2_31_x86_64.whl;; \
    esac;

RUN pip install nucliadb-node-binding>=0.5.2

RUN mkdir -p /usr/src/app

RUN pip install Cython==0.29.24 pybind11 gunicorn uvicorn uvloop


RUN curl -L -o /bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.1/grpc_health_probe-linux-amd64 && \
    chmod +x /bin/grpc_health_probe

# Install entrypoint.sh dependencies
RUN apt-get update && apt-get install -y jq

# Copy source code
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