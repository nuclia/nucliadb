FROM nuclia/nucliadb_rust_base:latest as python_rust

COPY . /nucliadb

WORKDIR /nucliadb

RUN set -eux; \
    cd nucliadb_node/binding; \
    maturin build --release

RUN cp /nucliadb/target/wheels/nucliadb_node*.whl /

# ---------------------------------------------------

FROM python:3.9

COPY --from=python_rust /tikv_client*.whl /
COPY --from=python_rust /nucliadb_node*.whl /

RUN pip install tikv_client-*.whl
RUN pip install nucliadb_node*.whl

RUN mkdir -p /usr/src/app

RUN pip install Cython==0.29.24 pybind11 gunicorn uvicorn uvloop

COPY nucliadb_utils/requirements.txt /usr/src/app/requirements-utils.txt
COPY nucliadb_utils/requirements-cache.txt /usr/src/app/requirements-cache.txt
COPY nucliadb_utils/requirements-storages.txt /usr/src/app/requirements-storages.txt
COPY nucliadb_utils/requirements-fastapi.txt /usr/src/app/requirements-fastapi.txt
COPY nucliadb_protos/python/requirements.txt /usr/src/app/requirements-protos.txt
COPY nucliadb_models/requirements.txt /usr/src/app/requirements-models.txt
COPY nucliadb_ingest/requirements.txt /usr/src/app/requirements-ingest.txt
COPY nucliadb_search/requirements.txt /usr/src/app/requirements-search.txt
COPY nucliadb_writer/requirements.txt /usr/src/app/requirements-writer.txt
COPY nucliadb_reader/requirements.txt /usr/src/app/requirements-reader.txt
COPY nucliadb_one/requirements.txt /usr/src/app/requirements-one.txt
COPY nucliadb_train/requirements.txt /usr/src/app/requirements-train.txt
COPY nucliadb_telemetry/requirements.txt /usr/src/app/requirements-telemetry.txt
COPY nucliadb/requirements.txt /usr/src/app/requirements.txt

RUN set -eux; \
    pip install --no-cache-dir \
    -r /usr/src/app/requirements-utils.txt \
    -r /usr/src/app/requirements-storages.txt \
    -r /usr/src/app/requirements-fastapi.txt \
    -r /usr/src/app/requirements-cache.txt \
    -r /usr/src/app/requirements-telemetry.txt \
    -r /usr/src/app/requirements-protos.txt \
    -r /usr/src/app/requirements-models.txt \
    -r /usr/src/app/requirements-ingest.txt \
    -r /usr/src/app/requirements-writer.txt \
    -r /usr/src/app/requirements-reader.txt \
    -r /usr/src/app/requirements-search.txt \
    -r /usr/src/app/requirements-one.txt \
    -r /usr/src/app/requirements-train.txt \
    -r /usr/src/app/requirements.txt

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
RUN pip install --no-deps -e /usr/src/app/nucliadb
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