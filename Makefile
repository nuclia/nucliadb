DOCKER_SERVICES ?= all

help:
	@grep '^[^#[:space:]].*:' Makefile

# Usage:
# `make docker-compose-start` starts all the services.
# `make docker-compose-start DOCKER_SERVICES='jaeger,localstack'` starts the subset of services matching the profiles.
docker-compose-deps-up:
	@echo "Launching ${DOCKER_SERVICES} Docker service(s)"
	COMPOSE_PROFILES=$(DOCKER_SERVICES) docker-compose -f docker-compose-deps.yml up --remove-orphans

docker-compose-deps-down:
	docker-compose -f docker-compose.yml down

docker-compose-deps-clean:
	docker-compose -f docker-compose.yml rm -v

docker-compose-deps-nats-clean:
	nats consumer del --server=localhost:4222 --force nucliadb-1 | true
	nats stream del --server=localhost:4222 --force nucliadb | true

docker-compose-deps-nats-init:
	nats stream add --server=localhost:4222 --subjects="nucliadb.*" --retention=limits --replicas=2 --discard=old --max-msgs=-1 --max-msgs-per-subject=-1 --max-msg-size=1000000 --max-bytes=100000000 --max-age=-1 --dupe-window=2m --storage=file nucliadb
	nats consumer add --server=localhost:4222 --filter=nucliadb.1  --target=ndb.consumer.1 --deliver=all --replay=instant --deliver-group=nucliadb-1 --ack=explicit --max-deliver=-1 --max-pending=1 --heartbeat=1s --flow-control nucliadb nucliadb-1

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

fmt:
	@echo "Formatting Rust files"
	@(rustup toolchain list | ( ! grep -q nightly && echo "Toolchain 'nightly' is not installed. Please install using 'rustup toolchain install nightly'.") ) || cargo +nightly fmt

protos: proto-py proto-rust

proto-py:
	python -m grpc_tools.protoc nucliadb_protos/noderesources.proto -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/utils.proto         -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/resources.proto     -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/knowledgebox.proto  -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/audit.proto 		-I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/nodewriter.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/nodereader.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/writer.proto        -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/

proto-rust:
	cargo build -p nucliadb_protos

proto-clean-py:
	rm -rf nucliadb_protos/nucliadb_protos/*.bak
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.pyi
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.pyi

python-code-lint:
	isort --profile black nucliadb_reader
	isort --profile black nucliadb_writer
	isort --profile black nucliadb_ingest
	isort --profile black nucliadb_utils
	isort --profile black nucliadb_models
	isort --profile black nucliadb_search
	isort --profile black nucliadb_one
	isort --profile black nucliadb_node
	isort --profile black nucliadb_telemetry

	flake8  --config nucliadb_reader/setup.cfg nucliadb_reader/nucliadb_reader
	flake8  --config nucliadb_writer/setup.cfg nucliadb_writer/nucliadb_writer
	flake8  --config nucliadb_ingest/setup.cfg nucliadb_ingest/nucliadb_ingest
	flake8  --config nucliadb_utils/setup.cfg nucliadb_utils/nucliadb_utils
	flake8  --config nucliadb_utils/setup.cfg nucliadb_models/nucliadb_models
	flake8  --config nucliadb_search/setup.cfg nucliadb_search/nucliadb_search
	flake8  --config nucliadb_one/setup.cfg nucliadb_one/nucliadb_one
	flake8  --config nucliadb_one/setup.cfg nucliadb_node/nucliadb_node
	flake8  --config nucliadb_telemetry/setup.cfg nucliadb_telemetry/nucliadb_telemetry

	black nucliadb_reader
	black nucliadb_writer
	black nucliadb_ingest
	black nucliadb_utils
	black nucliadb_models
	black nucliadb_search
	black nucliadb_one
	black nucliadb_node
	black nucliadb_telemetry

	MYPYPATH=./mypy_stubs mypy nucliadb_telemetry
	MYPYPATH=./mypy_stubs mypy nucliadb_utils
	MYPYPATH=./mypy_stubs mypy nucliadb_models
	MYPYPATH=./mypy_stubs mypy nucliadb_reader
	MYPYPATH=./mypy_stubs mypy nucliadb_writer
	MYPYPATH=./mypy_stubs mypy nucliadb_ingest
	MYPYPATH=./mypy_stubs mypy nucliadb_search
	MYPYPATH=./mypy_stubs mypy nucliadb_one
	MYPYPATH=./mypy_stubs mypy nucliadb_node
	

venv:  ## Initializes an environment
	pyenv virtualenv nucliadb
	pyenv local nucliadb

install: ## Install dependencies (on the active environment)
	pip install --upgrade pip
	pip install Cython==0.29.24
	pip install grpcio-tools
	pip install -r code-requirements.txt
	pip install -e ./nucliadb_utils
	pip install -e ./nucliadb_protos/python
	pip install -e ./nucliadb_models
	pip install -e ./nucliadb_ingest
	pip install -e ./nucliadb_reader
	pip install -e ./nucliadb_writer
	pip install -e ./nucliadb_search
	pip install -e ./nucliadb_telemetry
	pip install -r test-requirements.txt

base-node-image:
	docker buildx build --platform=linux/amd64 -t eu.gcr.io/stashify-218417/basenode:latest . -f Dockerfile.basenode
	docker push eu.gcr.io/stashify-218417/basenode:latest

build-local-node:
	docker build -t eu.gcr.io/stashify-218417/basenode:latest -f Dockerfile.basenode .
	docker build -t eu.gcr.io/stashify-218417/node:latest -f Dockerfile.node .

build-local-cluster-manager:
	docker build -t eu.gcr.io/stashify-218417/cluster_mgr:latest -f Dockerfile.cluster_monitor .
