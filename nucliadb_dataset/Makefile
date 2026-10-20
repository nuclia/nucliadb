include ../nucliadb.mk

.PHONY: format
format:
	ruff check --fix --config=../ruff.toml $(CODE_PATHS)
	ruff format --config=../ruff.toml $(CODE_PATHS)

.PHONY: lint
lint:
	ruff check --config=../ruff.toml $(CODE_PATHS)
	ruff format --check --config=../ruff.toml $(CODE_PATHS)
	ty check $(CODE_PATHS)
	mypy --config-file=../mypy.ini $(CODE_PATHS)

.PHONY: test
test:
	TEST_LOCAL_NUCLIADB=START $(PYTEST) tests/

.PHONY: test-cov
test-cov:
	TEST_LOCAL_NUCLIADB=START $(PYTEST) \
		$(pytest_cov_report_flags) \
		--cov=nucliadb_dataset \
		--cov=nucliadb_models \
		--cov=nucliadb_sdk \
		--cov-config=../.coveragerc \
		tests
