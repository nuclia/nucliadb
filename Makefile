help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

protos: proto-py

proto-py:
	uv sync --reinstall-package nucliadb_protos

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

debug-test-nucliadb:
	RUST_BACKTRACE=1 pytest nucliadb/tests -sxv

debug-run-nucliadb:
	RUST_BACKTRACE=1 MAX_RECEIVE_MESSAGE_LENGTH=1024 nucliadb --maindb=data/maindb --blob=data/blob --node=data/node --zone=europe-1 --log=DEBUG

debug-run-nucliadb-redis:
	nucliadb --driver=REDIS --maindb=redis://localhost:55359 --blob=data/blob --node=data/node --zone=europe-1 --log=INFO

build-nucliadb-local:
	docker build -t nuclia/nucliadb:latest . -f Dockerfile.withbinding
