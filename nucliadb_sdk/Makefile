.PHONY: format
format:
	ruff check --fix --config=../ruff.toml .
	ruff format --config=../ruff.toml .

.PHONY: lint
lint:
	ruff check --config=../ruff.toml .
	ruff format --check --config=../ruff.toml .
	mypy --config-file=../mypy.ini .

.PHONY: test
test:
	TEST_LOCAL_NUCLIADB=START pytest -rfE -sv --tb=native tests

.PHONY: test-cov
test-cov:
	TEST_LOCAL_NUCLIADB=START pytest \
		-rfE -sxv \
		--cov=nucliadb_sdk \
		--cov=nucliadb_models \
		--cov-config=../.coveragerc \
		--cov-report term-missing:skip-covered \
		--cov-report xml \
		--tb=native \
		tests
