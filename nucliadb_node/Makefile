.PHONY: check-system
check-system:
	../scripts/install-system-deps.sh


.PHONY: format
format:
	ruff check --fix --config=../ruff.toml .
	ruff format --config=../ruff.toml .

.PHONY: lint
lint:
	ruff check --config=../ruff.toml .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: test
test:
	pytest -sv --tb=native nucliadb_sidecar

.PHONY: test-cov
test-cov:
	pytest -rfE -sxv --cov=nucliadb_sidecar --cov-config=../.coveragerc --tb=native --cov-report term-missing:skip-covered --cov-report xml nucliadb_sidecar

.PHONY: build
build:
	cargo build --locked
