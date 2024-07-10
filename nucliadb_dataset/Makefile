.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r nucliadb_dataset/requirements-sources.txt \
		-r nucliadb_dataset/requirements.txt \
		-r nucliadb_dataset/requirements-test.txt
	pip install -e .

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
	pytest -rfE -s --tb=native -v

.PHONY: test-cov
test-cov:
	pytest -rfE --cov=nucliadb_dataset -s --tb=native -v --cov-report xml --cov-append tests
