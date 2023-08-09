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
	python -m grpc_tools.protoc nucliadb_protos/nodesidecar.proto   -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/nodewriter.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/nodereader.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/writer.proto        -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/train.proto         -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/dataset.proto       -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/migrations.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/

proto-rust:
	cargo build -p nucliadb_protos

proto-clean-py:
	rm -rf nucliadb_protos/nucliadb_protos/*.bak
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.pyi
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.pyi

python-code-lint:
	make -C nucliadb_dataset/ format
	make -C nucliadb_client/ format
	make -C nucliadb_models/ format
	make -C nucliadb_sdk/ format
	make -C nucliadb_node/ format
	make -C nucliadb_node_binding/ format
	make -C nucliadb_utils/ format
	make -C nucliadb/ format
	make -C nucliadb_telemetry/ format

	make -C nucliadb/ lint
	make -C nucliadb_utils/ lint
	make -C nucliadb_telemetry/ lint
	make -C nucliadb_sdk/ lint
	make -C nucliadb_dataset/ lint
	make -C nucliadb_client/ lint
	make -C nucliadb_models/ lint
	make -C nucliadb_node/ lint
	make -C nucliadb_node_binding/ lint


rust-code-lint:
	cargo +nightly fmt
	cargo clippy --tests


test-rust:
	cargo test --workspace --all-features --no-fail-fast


venv:  ## Initializes an environment
	pyenv virtualenv nucliadb
	pyenv local nucliadb

install: ## Install dependencies (on the active environment)
	pip install --upgrade pip wheel
	pip install Cython==0.29.24 grpcio-tools
	pip install -r code-requirements.txt -r test-requirements.txt
	pip install -e ./nucliadb_protos/python
	pip install -e ./nucliadb_telemetry
	pip install -e ./nucliadb_utils
	pip install -e ./nucliadb_models
	pip install -e ./nucliadb
	pip install -e ./nucliadb_client
	pip install -e ./nucliadb_sdk
	pip install -e ./nucliadb_dataset

base-node-image:
	docker buildx build --platform=linux/amd64 -t eu.gcr.io/stashify-218417/basenode:latest . -f Dockerfile.basenode
	docker push eu.gcr.io/stashify-218417/basenode:latest

build-search-images: build-local-node build-local-cluster-manager build-local-sidecar

build-node:
	docker build -t eu.gcr.io/stashify-218417/node:main -f Dockerfile.node .

# Not use the base image
build-base-node-image-scratch:
	docker build -t eu.gcr.io/stashify-218417/node:main -f Dockerfile.node_local .

build-local-cluster-manager:
	docker build -t eu.gcr.io/stashify-218417/cluster_manager:main -f Dockerfile.cluster_monitor .

build-local-sidecar:
	docker build -t eu.gcr.io/stashify-218417/node_sidecar:main -f Dockerfile.node_sidecar .


debug-test-nucliadb:
	RUST_BACKTRACE=1 RUST_LOG=nucliadb_node=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_fields_tantivy=DEBUG pytest nucliadb/nucliadb -sxv

debug-run-nucliadb:
	RUST_BACKTRACE=1 MAX_RECEIVE_MESSAGE_LENGTH=1024 RUST_LOG=nucliadb_node=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_fields_tantivy=DEBUG,nucliadb_vectors2=DEBUG nucliadb --maindb=data/maindb --blob=data/blob --node=data/node --zone=europe-1 --log=DEBUG

debug-run-nucliadb-redis:
	nucliadb --driver=REDIS --maindb=redis://localhost:55359 --blob=data/blob --node=data/node --zone=europe-1 --log=INFO


build-node-binding:
	rm -rf target/wheels/*
	maturin build -m nucliadb_node_binding/Cargo.toml --release
	pip install target/wheels/nucliadb_node_binding-*.whl --force

build-node-binding-debug:
	rm -rf target/wheels/*
	maturin build -m nucliadb_node_binding/Cargo.toml
	pip install target/wheels/nucliadb_node_binding-*.whl --force

build-nucliadb-local:
	docker build -t nuclia/nucliadb:latest .

build-nucliadb-local-withbinding:
	docker build -t nuclia/nucliadb:latest . -f Dockerfile.withbinding

build-nucliadb-rustbase_arm64:
	docker build -t nuclia/nucliadb_rust_base:arm64 . -f Dockerfile.rust
	docker push nuclia/nucliadb_rust_base:arm64

build-nucliadb-rustbase_amd64:
	docker build -t nuclia/nucliadb_rust_base:amd64 . -f Dockerfile.rust
	docker push nuclia/nucliadb_rust_base:amd64

link_docker_images:
	docker manifest create nuclia/nucliadb_rust_base:latest --amend nuclia/nucliadb_rust_base:amd64 --amend nuclia/nucliadb_rust_base:arm64
	docker manifest push nuclia/nucliadb_rust_base:latest

public_images:
	docker build -t eu.gcr.io/stashify-218417/basenode:latest . -f Dockerfile.basenode
	docker build -t nuclia/node:latest . -f Dockerfile.node
	docker build -t nuclia/node_sidecar:latest . -f Dockerfile.node_sidecar
	docker build -t nuclia/cluster_monitor:latest . -f Dockerfile.cluster_monitor
	docker push nuclia/node:latest
	docker push nuclia/node_sidecar:latest
	docker push nuclia/cluster_monitor:latest
