#
# This Dockerfile build nucliadb without the binding.
# See also `Dockerfile.withbinding` for a version with a compiled binding (for standalone)
#

# Stage to extract the external requirements from the lockfile
# This is to improve caching, `pdm.lock` changes when our components
# are updated. The generated requirements.txt does not, so it is
# more cacheable.
FROM python:3.12 AS requirements
RUN pip install pdm
COPY pdm.lock pyproject.toml .
RUN pdm export --prod --no-hashes | grep -v ^-e > requirements.lock.txt

FROM python:3.12
RUN mkdir -p /usr/src/app
RUN pip install pdm
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
COPY nucliadb_performance /usr/src/app/nucliadb_performance

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
