help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

protos: proto-py

proto-py:
	python -m grpc_tools.protoc nucliadb_protos/noderesources.proto -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/utils.proto         -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/resources.proto     -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/knowledgebox.proto  -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/audit.proto 		-I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/nodewriter.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/nodereader.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/backups.proto       -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/writer.proto        -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/train.proto         -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/dataset.proto       -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/migrations.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/standalone.proto    -I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/ --grpc_python_out=./nucliadb_protos/python/src/ --mypy_grpc_out=./nucliadb_protos/python/src/
	python -m grpc_tools.protoc nucliadb_protos/kb_usage.proto 		-I ./ --python_out=./nucliadb_protos/python/src/ --mypy_out=./nucliadb_protos/python/src/

proto-clean-py:
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2.pyi
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.py
	rm -rf nucliadb_protos/nucliadb_protos/*_pb2_grpc.pyi

python-code-lint:
	make -C nucliadb_dataset/ format
	make -C nucliadb_models/ format
	make -C nucliadb_sdk/ format
	make -C nucliadb_utils/ format
	make -C nucliadb/ format
	make -C nucliadb_telemetry/ format

	make -C nucliadb/ lint
	make -C nucliadb_utils/ lint
	make -C nucliadb_telemetry/ lint
	make -C nucliadb_sdk/ lint
	make -C nucliadb_dataset/ lint
	make -C nucliadb_models/ lint

rust-code-lint:
	cargo fmt --all --manifest-path nidx/Cargo.toml
	cargo clippy --all-features --manifest-path nidx/Cargo.toml


venv:  ## Initializes an environment
	pyenv virtualenv nucliadb
	pyenv local nucliadb

install:
	uv sync

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

debug-test-nucliadb:
	RUST_BACKTRACE=1 pytest nucliadb/tests -sxv

debug-run-nucliadb:
	RUST_BACKTRACE=1 MAX_RECEIVE_MESSAGE_LENGTH=1024 nucliadb --maindb=data/maindb --blob=data/blob --node=data/node --zone=europe-1 --log=DEBUG

debug-run-nucliadb-redis:
	nucliadb --driver=REDIS --maindb=redis://localhost:55359 --blob=data/blob --node=data/node --zone=europe-1 --log=INFO

build-nucliadb-local:
	docker build -t nuclia/nucliadb:latest . -f Dockerfile.withbinding
