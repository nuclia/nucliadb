.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r nucliadb_models/requirements-sources.txt \
		-r nucliadb_models/requirements.txt
	pip install -e .

.PHONY: format
format:
	cd .. && isort --profile black nucliadb_models
	black .

.PHONY: lint
lint:
	ruff check --config=../ruff.toml .
	cd .. && isort -c --profile black nucliadb_models
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .
