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
	pytest -sv --tb=native tests


.PHONY: test-cov
test-cov:
	pytest -rfE -sxv --cov=nucliadb_models --cov-config=../.coveragerc --tb=native --cov-report term-missing:skip-covered --cov-report xml tests
