help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

protos: proto-py

proto-py:
	uv sync --reinstall-package nucliadb_protos --reinstall-package nidx_protos

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

install:
	uv sync

debug-test-nucliadb:
	RUST_BACKTRACE=1 pytest nucliadb/tests -sxv

build-nucliadb-local:
	docker build -t nuclia/nucliadb:latest . -f Dockerfile.withbinding

build-nidx: # Required for tests
	docker build -t nidx:latest . -f Dockerfile.nidx