#
# This Dockerfile build nucliadb without the binding.
# See also `Dockerfile.withbinding` for a version with a compiled binding (for standalone)
#

# Stage to extract the external requirements from the lockfile
# This is to improve caching, `uv.lock` changes when our components
# are updated. The generated requirements.txt does not, so it is
# more cacheable.
FROM python:3.12-slim-bookworm AS requirements
RUN pip install uv
COPY uv.lock pyproject.toml .
RUN uv export --no-sources --frozen --no-emit-workspace > requirements.lock.txt

#
# This stage builds a virtual env with all dependencies
#
FROM python:3.12-slim-bookworm AS builder
RUN mkdir -p /usr/src/app
RUN pip install uv

# Install Python dependencies
WORKDIR /usr/src/app
COPY --from=requirements requirements.lock.txt /tmp
RUN pip install uv && python -m venv /app && VIRTUAL_ENV=/app uv pip install -r /tmp/requirements.lock.txt

# Copy application
COPY VERSION pyproject.toml uv.lock /usr/src/app
COPY nucliadb_utils /usr/src/app/nucliadb_utils
COPY nucliadb_telemetry /usr/src/app/nucliadb_telemetry
COPY nucliadb_protos /usr/src/app/nucliadb_protos
COPY nucliadb_models /usr/src/app/nucliadb_models
COPY nucliadb /usr/src/app/nucliadb
COPY nidx/nidx_protos /usr/src/app/nidx/nidx_protos

# Copy stubs for packages we won't install
COPY nidx/nidx_binding/pyproject.toml /usr/src/app/nidx/nidx_binding/pyproject.toml
COPY nucliadb_sdk/pyproject.toml /usr/src/app/nucliadb_sdk/pyproject.toml
COPY nucliadb_dataset/pyproject.toml /usr/src/app/nucliadb_dataset/pyproject.toml

# Install our packages to the virtualenv
RUN VIRTUAL_ENV=/app uv sync --active --no-editable --no-group nidx --no-group sdk

#
# This is the main image, it just copies the virtual env into the base image
#
FROM python:3.12-slim-bookworm
# media-types needed for content type checks (python mimetype module uses this)
RUN apt update && apt install -y media-types && apt clean && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app /app

# HTTP
EXPOSE 8080/tcp
# GRPC - INGEST
EXPOSE 8060/tcp
# GRPC - TRAIN
EXPOSE 8040/tcp

ENV PATH="/app/bin:$PATH"
WORKDIR /app
CMD ["nucliadb"]
