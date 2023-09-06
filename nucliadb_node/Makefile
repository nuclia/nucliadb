.PHONY: install-dev
install-dev: check-system
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt \
		-r nucliadb_node/requirements-sources.txt \
		-r nucliadb_node/requirements.txt
	pip install -e .

.PHONY: check-system
check-system:
	../scripts/install-system-deps.sh


.PHONY: format
format:
	cd .. && isort --profile black nucliadb_node
	black .

.PHONY: lint
lint:
	flake8 nucliadb_node --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_node
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
	export RUSTFLAGS="--cfg=prometheus_metrics" && cargo build --locked
