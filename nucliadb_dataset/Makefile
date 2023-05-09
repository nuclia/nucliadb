.PHONY: install-dev
install-dev:
## TODO: remove pip version install when pip is fixed. Right now it was making mypy fail.
	pip install --upgrade "pip<23.1"
	pip install -r ../test-requirements.txt
	pip install -r ../code-requirements.txt
	cd .. && pip install -r nucliadb_dataset/requirements-sources.txt
	pip install -r requirements.txt
	cd .. && pip install -r nucliadb_dataset/requirements-test.txt
	pip install -e .

.PHONY: format
format:
	cd .. && isort --profile black nucliadb_dataset
	black .

.PHONY: lint
lint:
	flake8 nucliadb_dataset --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_dataset
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: test
test:
	pytest -rfE --cov=nucliadb_dataset -s --tb=native -v --cov-report xml --cov-append nucliadb_dataset
