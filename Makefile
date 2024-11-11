help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

fmt-all:
	@echo "Formatting Rust files"
	cargo fmt

fmt-check-package:
	@echo "Formatting Rust files from specific package"
	cargo fmt -p $(PACKAGE) --check


protos: proto-py proto-rust

proto-py:
	python -m grpc_tools.protoc nucliadb_protos/noderesources.proto -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/utils.proto         -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/resources.proto     -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/knowledgebox.proto  -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/audit.proto 		-I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/nodewriter.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/nodereader.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/writer.proto        -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/train.proto         -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/dataset.proto       -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/migrations.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/standalone.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/replication.proto   -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/kb_usage.proto 		-I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/

proto-rust:
	cargo build --locked -p nucliadb_protos

proto-clean-py:
	rm -rf nucliadb_protos/nucliadb_protos/*.bak
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.pyi
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.pyi

python-code-lint:
	make -C nucliadb_dataset/ format
	make -C nucliadb_models/ format
	make -C nucliadb_sdk/ format
	make -C nucliadb_sidecar/ format
	make -C nucliadb_node_binding/ format
	make -C nucliadb_utils/ format
	make -C nucliadb/ format
	make -C nucliadb_telemetry/ format
	make -C nucliadb_performance/ format

	make -C nucliadb/ lint
	make -C nucliadb_utils/ lint
	make -C nucliadb_telemetry/ lint
	make -C nucliadb_sdk/ lint
	make -C nucliadb_dataset/ lint
	make -C nucliadb_models/ lint
	make -C nucliadb_sidecar/ lint
	make -C nucliadb_node_binding/ lint
	make -C nucliadb_performance/ lint


rust-code-lint: fmt-all
	cargo clippy --tests


test-rust:
	cargo test --workspace --all-features --no-fail-fast


venv:  ## Initializes an environment
	pyenv virtualenv nucliadb
	pyenv local nucliadb

install:
	pdm sync -d -G :all

install__deprecated: ## Install dependencies (on the active environment)
	pip install --upgrade pip wheel
	pip install "Cython==0.29.24" "grpcio-tools>=1.44.0,<1.63.0"
	pip install -r test-requirements.txt
	pip install -e ./nucliadb_protos/python
	pip install -e ./nucliadb_telemetry
	pip install -e ./nucliadb_utils
	pip install -e ./nucliadb_models
	pip install -e ./nucliadb
	pip install -e ./nucliadb_sdk
	pip install -e ./nucliadb_dataset

build-node:
	docker build -t europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node:latest -f Dockerfile.node .

build-node-prebuilt:
	cargo build --release --bin node_reader --bin node_writer
	mkdir builds || true
	cp target/release/node_*er builds
	docker build -t europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node:latest -f Dockerfile.node_prebuilt .

build-sidecar:
	docker build -t europe-west4-docker.pkg.dev/nuclia-internal/nuclia/node_sidecar:latest -f Dockerfile.node_sidecar .


debug-test-nucliadb:
	RUST_BACKTRACE=1 RUST_LOG=nucliadb_node=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_fields_tantivy=DEBUG pytest nucliadb/tests -sxv

debug-run-nucliadb:
	RUST_BACKTRACE=1 MAX_RECEIVE_MESSAGE_LENGTH=1024 RUST_LOG=nucliadb_node=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_fields_tantivy=DEBUG nucliadb --maindb=data/maindb --blob=data/blob --node=data/node --zone=europe-1 --log=DEBUG

debug-run-nucliadb-redis:
	nucliadb --driver=REDIS --maindb=redis://localhost:55359 --blob=data/blob --node=data/node --zone=europe-1 --log=INFO


build-node-binding:
	rm -rf target/wheels/*
	RUSTFLAGS="--cfg tokio_unstable" maturin build -m nucliadb_node_binding/Cargo.toml --profile release-wheel
	pip install target/wheels/nucliadb_node_binding-*.whl --force

build-node-binding-debug:
	rm -rf target/wheels/*
	RUSTFLAGS="--cfg tokio_unstable" maturin build -m nucliadb_node_binding/Cargo.toml
	pip install target/wheels/nucliadb_node_binding-*.whl --force

build-nucliadb-local:
	docker build -t nuclia/nucliadb:latest . -f Dockerfile.pipbinding

build-nucliadb-local-withbinding:
	docker build -t nuclia/nucliadb:latest . -f Dockerfile.withbinding
