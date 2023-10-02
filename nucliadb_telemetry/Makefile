
protos:
	python -m grpc_tools.protoc ./nucliadb_telemetry/tests/grpc/helloworld.proto           -I ./ --python_out=./ --mypy_out=./ --grpc_python_out=./ --mypy_grpc_out=./
	python -m grpc_tools.protoc ./nucliadb_telemetry/tests/grpc/hellostreamingworld.proto  -I ./ --python_out=./ --mypy_out=./ --grpc_python_out=./ --mypy_grpc_out=./

.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	pip install -r ../test-requirements.txt -r ../code-requirements.txt -r requirements.txt
# utils dep required for test fixtures for nats
# NEEDS TO BE DECOUPLED EVENTUALLY
	pip install -e ../nucliadb_protos/python
	pip install -e ../nucliadb_utils
	pip install -e .

.PHONY: format
format:
	cd .. && isort --profile black nucliadb_telemetry
	black .

.PHONY: lint
lint:
	flake8 nucliadb_telemetry --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_telemetry
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini . --show-traceback

.PHONY: test
test:
	pytest -s --tb=native -v nucliadb_telemetry

.PHONY: test-cov
test-cov:
	pytest -rfE --cov=nucliadb_telemetry --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml nucliadb_telemetry
