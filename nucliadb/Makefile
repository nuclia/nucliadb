subpackages := standalone reader search train writer ingest

format_target_prefix := format-
lint_target_prefix := lint-
test_target_prefix := test-
test_cov_target_prefix := test-cov-

format_targets = $(addprefix $(format_target_prefix),$(subpackages))
lint_targets = $(addprefix $(lint_target_prefix),$(subpackages))
test_targets = $(addprefix $(test_target_prefix),$(subpackages))
test_cov_targets = $(addprefix $(test_cov_target_prefix),$(subpackages))

pytest_flags := -s -rfE -v --tb=native
pytest_extra_flags :=

PYTEST := pytest $(pytest_flags) $(pytest_extra_flags)


.PHONY: install-dev
install-dev: check-system
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt \
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

$(format_targets):


.PHONY: lint
lint:
	flake8 nucliadb --config=setup.cfg
	cd .. && isort -c --profile black nucliadb
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini .

$(lint_targets):

# pytest doesn't support test discovery across our multiple "top-level folders"
# so we need to run all test targets one by one
.PHONY: test-all
test-all: $(test_targets)

test-nucliadb:
	$(PYTEST) nucliadb/tests

$(test_targets):

$(test_cov_targets):


# Generic implicit rules

format-%:
	cd .. && isort --profile black nucliadb/nucliadb/$*
	black nucliadb/$*

lint-%:
	flake8 nucliadb/$* --config=setup.cfg
	cd .. && isort -c --profile black nucliadb/nucliadb/$*
	black --check nucliadb/$*
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini nucliadb/$*

test-%:
	$(PYTEST) nucliadb/$*/tests

test-cov-%:
	$(PYTEST) \
		--cov=nucliadb.$* \
		--cov-config=../.coveragerc \
		--cov-report xml \
		--cov-report term-missing:skip-covered \
		--cov-append \
		nucliadb/$*/tests
