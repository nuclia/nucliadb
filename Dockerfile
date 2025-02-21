#
# This Dockerfile build nucliadb without the binding.
# See also `Dockerfile.withbinding` for a version with a compiled binding (for standalone)
#

# Stage to extract the external requirements from the lockfile
# This is to improve caching, `pdm.lock` changes when our components
# are updated. The generated requirements.txt does not, so it is
# more cacheable.
FROM python:3.12 AS requirements
RUN pip install pdm==2.22.1
COPY pdm.lock pyproject.toml .
RUN pdm export --prod --no-hashes | grep -v ^-e | grep -v nidx_protos > requirements.lock.txt

FROM python:3.12
RUN mkdir -p /usr/src/app
RUN pip install pdm==2.22.1

# Install Python dependencies
WORKDIR /usr/src/app
COPY --from=requirements requirements.lock.txt /tmp
RUN python -m venv .venv && \
    .venv/bin/pip install -r /tmp/requirements.lock.txt

# Copy application
COPY VERSION pyproject.toml pdm.lock /usr/src/app
COPY nucliadb_utils /usr/src/app/nucliadb_utils
COPY nucliadb_telemetry /usr/src/app/nucliadb_telemetry
COPY nucliadb_protos /usr/src/app/nucliadb_protos
COPY nucliadb_models /usr/src/app/nucliadb_models
COPY nucliadb /usr/src/app/nucliadb
COPY nidx/nidx_protos /usr/src/app/nidx/nidx_protos

# Create a fake binding to avoid installing the real one
RUN mkdir nucliadb_node_binding && \
    touch nucliadb_node_binding/pyproject.toml

# Install our packages to the virtualenv
RUN pdm sync --prod --no-editable

RUN mkdir -p /data

ENV NUA_ZONE=europe-1
ENV NUA_API_KEY=
ENV NUCLIA_PUBLIC_URL=https://{zone}.nuclia.cloud
ENV PYTHONUNBUFFERED=1
ENV DRIVER=LOCAL
ENV HTTP_PORT=8080
ENV INGEST_GRPC_PORT=8060
ENV TRAIN_GRPC_PORT=8040
ENV LOG_OUTPUT_TYPE=stdout

# HTTP
EXPOSE 8080/tcp
# GRPC - INGEST
EXPOSE 8060/tcp
# GRPC - TRAIN
EXPOSE 8040/tcp

ENV PATH="/usr/src/app/.venv/bin:$PATH"
CMD ["nucliadb"]
