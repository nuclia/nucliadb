include ../nucliadb.mk

protos:
	python -m grpc_tools.protoc ./nucliadb_telemetry/tests/grpc/helloworld.proto           -I ./ --python_out=./ --mypy_out=./ --grpc_python_out=./ --mypy_grpc_out=./
	python -m grpc_tools.protoc ./nucliadb_telemetry/tests/grpc/hellostreamingworld.proto  -I ./ --python_out=./ --mypy_out=./ --grpc_python_out=./ --mypy_grpc_out=./

.PHONY: format
format:
	ruff check --fix --config=../ruff.toml $(CODE_PATHS)
	ruff format --config=../ruff.toml $(CODE_PATHS)

.PHONY: lint
lint:
	ruff check --config=../ruff.toml $(CODE_PATHS)
	ruff format --check --config=../ruff.toml $(CODE_PATHS)
	ty check $(CODE_PATHS)
	mypy --config-file=../mypy.ini $(CODE_PATHS) --show-traceback

.PHONY: test
test:
	$(PYTEST) tests/

.PHONY: test-cov
test-cov:
	$(PYTEST) $(pytest_cov_report_flags) --cov=nucliadb_telemetry --cov-config=../.coveragerc tests/
