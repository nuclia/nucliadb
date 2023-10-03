.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt \
		-r nucliadb_sdk/requirements-sources.txt \
		-r nucliadb_sdk/requirements.txt \
		-r nucliadb_sdk/requirements-test.txt
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
