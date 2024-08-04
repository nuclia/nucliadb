SHELL := /bin/bash

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
	pytest -sv --tb=native tests

# TODO rather complex set up, needs to be done globally
.PHONY: install-cov
install-cov:
	rustup component add llvm-tools-preview
	- cargo install cargo-llvm-cov

.PHONY: test-cov
# test-cov:
# 	bash ./cov.sh
test-cov: # Without coverage for now, since it's currently broken
	pytest -sxv --tb=native tests

.PHONY: build
build:
	maturin build
