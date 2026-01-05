include ../nucliadb.mk

.PHONY: format
format:
	ruff check --fix --config=../ruff.toml .
	ruff format --config=../ruff.toml .


.PHONY: lint
lint:
	ruff check --config=../ruff.toml .
	ruff format --check --config=../ruff.toml .
	mypy --config-file=../mypy.ini src

.PHONY: test
test:
	$(PYTEST) tests/

.PHONY: test-cov
test-cov:
	$(PYTEST) $(pytest_cov_report_flags) --cov=nucliadb_utils --cov-config=../.coveragerc tests/
