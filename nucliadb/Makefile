subpackages := nucliadb standalone reader search train writer ingest

test_target_prefix := test-
test_cov_target_prefix := test-cov-

test_subpackages_targets = $(addprefix $(test_target_prefix),$(subpackages))
test_cov_subpackages_targets = $(addprefix $(test_cov_target_prefix),$(subpackages))

pytest_flags := -s -rfE -v --tb=native
pytest_extra_flags :=

PYTEST := pytest $(pytest_flags) $(pytest_extra_flags)


.PHONY: install-dev
install-dev: check-system
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r nucliadb/requirements-sources.txt \
		-r nucliadb/requirements.txt \
		-r nucliadb/requirements-test.txt
	pip install -e .

.PHONY: check-system
check-system:
	../scripts/install-system-deps.sh

freeze-requirements:
# create venv only for freezing requirements
	python -m venv .freeze-venv
	.freeze-venv/bin/pip install --upgrade pip wheel
	cd .. && nucliadb/.freeze-venv/bin/pip install \
		-r nucliadb/requirements.txt \
		-r nucliadb/requirements-sources.txt
	.freeze-venv/bin/pip install -e .
	.freeze-venv/bin/pip freeze > requirements.lock.txt.raw
	rm -rf .freeze-venv
# filter out all nucliadb packages from requirements.lock.txt
	grep -v 'nucliadb' requirements.lock.txt.raw > requirements.lock.txt
	rm requirements.lock.txt.raw


# Global format and lint are preferred in a single process as it's quite faster
# than executing each subpackage target

.PHONY: format
format:
	cd .. && isort --profile black nucliadb
	black .
	ruff check --fix --config=../ruff.toml .


.PHONY: lint
lint:
	ruff check . --config=../ruff.toml
	cd .. && isort -c --profile black nucliadb
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

# pytest doesn't support test discovery across our multiple "top-level folders"
# so we need to run all test targets one by one
.PHONY: test-all
test-all: $(test_subpackages_targets)

$(test_subpackages_targets):

$(test_cov_subpackages_targets):


# Generic implicit rules

test-%:
	$(PYTEST) tests/$*

test-cov-%:
	$(PYTEST) \
		--cov=nucliadb.$* \
		--cov-config=../.coveragerc \
		--cov-report xml \
		--cov-report term-missing:skip-covered \
		--cov-append \
		tests/$*
