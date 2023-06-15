.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt \
		-r nucliadb_node/requirements-sources.txt \
		-r nucliadb_node/requirements.txt
	pip install -e .

.PHONY: format
format:
	isort --profile black .
	black .

.PHONY: lint
lint:
	flake8 nucliadb_node --config=setup.cfg
	isort -c --profile black .
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

.PHONY: test
test:
	pytest -s --tb=native -v nucliadb_node

.PHONY: test-cov
test-cov:
	pytest -rfE --cov=nucliadb_node --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml nucliadb_node

.PHONY: build
build:
	export RUSTFLAGS="--cfg=prometheus_metrics" && cargo build
