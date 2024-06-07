SHELL := /bin/bash

.PHONY: install-dev
install-dev: check-system
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r nucliadb_node/requirements-sources.txt \
		-r nucliadb_node/requirements.txt \
		./nucliadb_node/
	pip install -e .

.PHONY: check-system
check-system:
	../scripts/install-system-deps.sh


.PHONY: format
format:
	ruff check --fix --config=../ruff.toml tests
	ruff format --config=../ruff.toml tests

.PHONY: lint
lint:
	ruff check --config=../ruff.toml tests
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: test
test:
	pytest -s --tb=native -v tests

# TODO rather complex set up, needs to be done globally
.PHONY: install-cov
install-cov:
	rustup component add llvm-tools-preview
	- cargo install cargo-llvm-cov

.PHONY: test-cov
test-cov:
	bash ./cov.sh

.PHONY: build
build:
	maturin build
