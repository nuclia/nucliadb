.PHONY: format
format:
	ruff check --fix --config=../ruff.toml .
	ruff format --config=../ruff.toml .


.PHONY: lint
lint:
	ruff check --config=../ruff.toml .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini src

.PHONY: test
test:
	pytest -s --tb=native -v tests

.PHONY: test-cov
test-cov:
	pytest -rfE --cov=nucliadb_utils --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml tests
