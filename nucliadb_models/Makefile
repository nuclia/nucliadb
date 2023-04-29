.PHONY: install-dev
install-dev:
## TODO: remove pip version install when pip is fixed. Right now it was making mypy fail.
	pip install --upgrade "pip<23.1"
	pip install -r ../test-requirements.txt
	pip install -r ../code-requirements.txt
	pip install -r requirements.txt
	pip install -e .

.PHONY: format
format:
	cd .. && isort --profile black nucliadb_models
	black .

.PHONY: lint
lint:
	flake8 nucliadb_models --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_models
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

