.PHONY: install-dev
install-dev:
## TODO: remove pip version install when pip is fixed. Right now it was making mypy fail.
	pip install --upgrade "pip<23.1"
	pip install -r ../test-requirements.txt
	pip install -r ../code-requirements.txt
	cd .. && pip install -r nucliadb/requirements-sources.txt
	pip install -r requirements.txt
	pip install -r requirements-test.txt
	pip install -e .

.PHONY: format
format:
	cd .. && isort --profile black nucliadb
	black .

.PHONY: lint
lint:
	flake8 nucliadb --config=setup.cfg
	cd .. && isort -c --profile black nucliadb
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: lint-one
lint-one:
	flake8 nucliadb/one --config=setup.cfg
	cd .. && isort -c --profile black nucliadb/nucliadb/one
	black --check nucliadb/one
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini nucliadb/one

.PHONY: lint-reader
lint-reader:
	flake8 nucliadb/reader --config=setup.cfg
	cd .. && isort -c --profile black nucliadb/nucliadb/reader
	black --check nucliadb/reader
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini nucliadb/reader

.PHONY: lint-search
lint-search:
	flake8 nucliadb/search --config=setup.cfg
	cd .. && isort -c --profile black nucliadb/nucliadb/search
	black --check nucliadb/search
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini nucliadb/search

.PHONY: lint-train
lint-train:
	flake8 nucliadb/train --config=setup.cfg
	cd .. && isort -c --profile black nucliadb/nucliadb/train
	black --check nucliadb/train
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini nucliadb/train

.PHONY: lint-writer
lint-writer:
	flake8 nucliadb/writer --config=setup.cfg
	cd .. && isort -c --profile black nucliadb/nucliadb/writer
	black --check nucliadb/writer
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini nucliadb/writer

.PHONY: test-ingest
test-ingest:
	pytest -s --tb=native -v nucliadb/ingest

.PHONY: test-cov-ingest
test-cov-ingest:
	pytest -rfE --cov=nucliadb.ingest --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml nucliadb/ingest

.PHONY: test
test:
	pytest -rfE --cov=nucliadb -s --tb=native -v --cov-report xml --cov-append --benchmark-json benchmarks.json nucliadb/nucliadb/tests
