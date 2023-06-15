.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt \
		-r nucliadb_dataset/requirements-sources.txt \
		-r nucliadb_dataset/requirements.txt \
		-r nucliadb_dataset/requirements-test.txt
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
