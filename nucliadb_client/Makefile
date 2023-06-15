.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt \
		-r nucliadb_client/requirements-source.txt \
		-r nucliadb_client/requirements.txt \
		-r nucliadb_client/requirements-test.txt
	pip install -e .

.PHONY: format
format:
	cd .. && isort --profile black nucliadb_client
	black .

.PHONY: lint
lint:
	flake8 nucliadb_client --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_client
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: test
test:
	pytest -rfE --cov=nucliadb_client -s --tb=native -v --cov-report xml --cov-append nucliadb_client
