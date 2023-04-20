.PHONY: install-dev
install-dev:
## TODO: remove pip version install when pip is fixed. Right now it was making mypy fail.
	pip install --upgrade "pip<23.1"
	pip install -r ../test-requirements.txt
	pip install -r ../code-requirements.txt
	cd .. && pip install -r nucliadb_sdk/requirements-sources.txt
	pip install -r requirements.txt
	pip install -r requirements-test.txt
	pip install -e ../nucliadb_client/
	pip install -e .

.PHONY: format
format:
	cd .. && isort --profile black nucliadb_sdk
	black .

.PHONY: lint
lint:
	flake8 nucliadb_sdk --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_sdk
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: test
test:
	pytest -s --tb=native -v nucliadb_sdk
