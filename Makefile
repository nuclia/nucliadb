help:
	@grep '^[^#[:space:]].*:' Makefile

license-check:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header check

license-fix:
	docker run -it --rm -v $(shell pwd):/github/workspace ghcr.io/apache/skywalking-eyes/license-eye header fix

protos: proto-py

proto-py:
	uv sync --reinstall-package nucliadb_protos --reinstall-package nidx_protos --inexact

.PHONY: python-code-lint python-code-lint-src
python-code-lint-src:
	$(MAKE) python-code-lint CODE_PATHS=src

python-code-lint:
	$(MAKE) -C nucliadb_dataset/ format
	$(MAKE) -C nucliadb_models/ format
	$(MAKE) -C nucliadb_sdk/ format
	$(MAKE) -C nucliadb_utils/ format
	$(MAKE) -C nucliadb/ format
	$(MAKE) -C nucliadb_telemetry/ format

	$(MAKE) -C nucliadb/ lint
	$(MAKE) -C nucliadb_utils/ lint
	$(MAKE) -C nucliadb_telemetry/ lint
	$(MAKE) -C nucliadb_sdk/ lint
	$(MAKE) -C nucliadb_dataset/ lint
	$(MAKE) -C nucliadb_models/ lint

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
