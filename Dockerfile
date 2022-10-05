FROM python:3.9

COPY --from=nuclia/nucliadb_rust_base:latest /tikv_client*.whl /

RUN pip install tikv_client-*.whl
RUN pip install nucliadb-node-binding

RUN mkdir -p /usr/src/app

RUN pip install Cython==0.29.24 pybind11 gunicorn uvicorn uvloop

# Copy source code
COPY nucliadb_utils /usr/src/app/nucliadb_utils
COPY nucliadb_telemetry /usr/src/app/nucliadb_telemetry
COPY nucliadb_protos /usr/src/app/nucliadb_protos
COPY nucliadb_models /usr/src/app/nucliadb_models
COPY nucliadb_ingest /usr/src/app/nucliadb_ingest
COPY nucliadb_search /usr/src/app/nucliadb_search
COPY nucliadb_writer /usr/src/app/nucliadb_writer
COPY nucliadb_reader /usr/src/app/nucliadb_reader
COPY nucliadb_one /usr/src/app/nucliadb_one
COPY nucliadb_train /usr/src/app/nucliadb_train
COPY nucliadb /usr/src/app/nucliadb

WORKDIR /usr/src/app

# Install all dependendencies on packages on the nucliadb repo
# and finally the main component.
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