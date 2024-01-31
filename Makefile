help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

# we are pinning rustfmt to 1.6.0-nightly
check-rustfmt:
	@cargo +nightly fmt --version | grep "1.6.0" > /dev/null || (rustup component add rustfmt --toolchain nightly && rustup upgrade)

fmt-all: check-rustfmt
	@echo "Formatting Rust files"
	cargo +nightly fmt

fmt-check-package: check-rustfmt
	@echo "Formatting Rust files from specific package"
	cargo +nightly fmt -p $(PACKAGE) --check


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
	python -m grpc_tools.protoc nucliadb_protos/train.proto         -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/dataset.proto       -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/migrations.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/standalone.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/
	python -m grpc_tools.protoc nucliadb_protos/replication.proto    -I ./ --python_out=./nucliadb_protos/python/ --mypy_out=./nucliadb_protos/python/ --grpc_python_out=./nucliadb_protos/python/ --mypy_grpc_out=./nucliadb_protos/python/

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
	make -C nucliadb_node/ format
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
	make -C nucliadb_node/ lint
	make -C nucliadb_node_binding/ lint
	make -C nucliadb_performance/ lint


rust-code-lint: fmt-all
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
	pip install -e ./nucliadb_sdk
	pip install -e ./nucliadb_dataset

base-node-image:
	docker buildx build --platform=linux/amd64 -t eu.gcr.io/stashify-218417/basenode:latest . -f Dockerfile.basenode
	docker push eu.gcr.io/stashify-218417/basenode:latest
	docker tag eu.gcr.io/stashify-218417/basenode:latest europe-west4-docker.pkg.dev/nuclia-internal/private/basenode:latest
	docker push europe-west4-docker.pkg.dev/nuclia-internal/private/basenode:latest
	docker tag eu.gcr.io/stashify-218417/basenode:latest 042252809363.dkr.ecr.us-east-2.amazonaws.com/basenode:latest
	docker push 042252809363.dkr.ecr.us-east-2.amazonaws.com/basenode:latest

build-node:
	docker build -t eu.gcr.io/stashify-218417/node:latest -f Dockerfile.node .

build-node-debug:
	./scripts/download-build.sh && echo "Using build server build" && docker build -t eu.gcr.io/stashify-218417/node:latest -f Dockerfile.node_prebuilt . || ( \
		echo "Failed to download build from build server. Manually running build." && \
		docker build -t eu.gcr.io/stashify-218417/node:latest --build-arg CARGO_PROFILE=debug -f Dockerfile.node . \
	)


# Not use the base image
build-base-node-image-scratch:
	docker build -t eu.gcr.io/stashify-218417/node:latest -f Dockerfile.node_local .

build-sidecar:
	docker build -t eu.gcr.io/stashify-218417/node_sidecar:latest -f Dockerfile.node_sidecar .


debug-test-nucliadb:
	RUST_BACKTRACE=1 RUST_LOG=nucliadb_node=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_fields_tantivy=DEBUG pytest nucliadb/nucliadb -sxv

debug-run-nucliadb:
	RUST_BACKTRACE=1 MAX_RECEIVE_MESSAGE_LENGTH=1024 RUST_LOG=nucliadb_node=DEBUG,nucliadb_paragraphs_tantivy=DEBUG,nucliadb_fields_tantivy=DEBUG,nucliadb_vectors2=DEBUG nucliadb --maindb=data/maindb --blob=data/blob --node=data/node --zone=europe-1 --log=DEBUG

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
	docker build -t nuclia/nucliadb:latest .

build-nucliadb-local-withbinding:
	docker build -t nuclia/nucliadb:latest . -f Dockerfile.withbinding
