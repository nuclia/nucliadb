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
	TEST_LOCAL_NUCLIADB=START pytest -rfE -s --tb=native -v tests

.PHONY: test-cov
test-cov:
	TEST_LOCAL_NUCLIADB=START pytest -rfE --cov=nucliadb_dataset --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml --cov-append tests
