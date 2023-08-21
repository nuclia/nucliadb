.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt \
		-r nucliadb_utils/requirements-sources.txt \
		-r nucliadb_utils/requirements.txt
	pip install -e .[cache,storages,fastapi,postgres]

.PHONY: format
format:
	cd .. && isort --profile black nucliadb_utils
	black .

.PHONY: lint
lint:
	flake8 nucliadb_utils --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_utils
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: test
test:
	pytest -s --tb=native -v nucliadb_utils

.PHONY: test-cov
test-cov:
	pytest -rfE --cov=nucliadb_utils --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml nucliadb_utils
