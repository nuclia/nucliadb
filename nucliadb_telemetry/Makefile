
protos:
	python -m grpc_tools.protoc ./nucliadb_telemetry/tests/grpc/helloworld.proto           -I ./ --python_out=./ --mypy_out=./ --grpc_python_out=./ --mypy_grpc_out=./
	python -m grpc_tools.protoc ./nucliadb_telemetry/tests/grpc/hellostreamingworld.proto  -I ./ --python_out=./ --mypy_out=./ --grpc_python_out=./ --mypy_grpc_out=./

.PHONY: format
format:
	ruff check --fix --config=../ruff.toml .
	ruff format --config=../ruff.toml .

.PHONY: lint
lint:
	ruff check --config=../ruff.toml .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini . --show-traceback

.PHONY: test
test:
	pytest -s --tb=native -v tests

.PHONY: test-cov
test-cov:
	pytest -rfE --cov=nucliadb_telemetry --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml tests
