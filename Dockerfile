#
# This Dockerfile build nucliadb without the binding.
# See also `Dockerfile.withbinding` for a version with a compiled binding (for standalone)
#

# Stage to extract the external requirements from the lockfile
# This is to improve caching, `pdm.lock` changes when our components
# are updated. The generated requirements.txt does not, so it is
# more cacheable.
FROM python:3.12-slim-bookworm AS requirements
RUN pip install pdm>=2.22.3
COPY pdm.lock pyproject.toml .
RUN pdm export --prod --no-hashes | grep -v ^-e > requirements.lock.txt

#
# This stage builds a virtual env with all dependencies
#
FROM python:3.12-slim-bookworm AS builder
RUN mkdir -p /usr/src/app
RUN pip install pdm>=2.22.3

# Install Python dependencies
WORKDIR /usr/src/app
COPY --from=requirements requirements.lock.txt /tmp
RUN python -m venv /app && /app/bin/pip install -r /tmp/requirements.lock.txt

# Copy application
COPY VERSION pyproject.toml pdm.lock /usr/src/app
COPY nucliadb_utils /usr/src/app/nucliadb_utils
COPY nucliadb_telemetry /usr/src/app/nucliadb_telemetry
COPY nucliadb_protos /usr/src/app/nucliadb_protos
COPY nucliadb_models /usr/src/app/nucliadb_models
COPY nucliadb /usr/src/app/nucliadb
COPY nidx/nidx_protos /usr/src/app/nidx/nidx_protos

# Install our packages to the virtualenv
RUN pdm use -f /app && pdm sync --prod --no-editable

#
# This is the main image, it just copies the virtual env into the base image
#
FROM python:3.12-slim-bookworm
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
