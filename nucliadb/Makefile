subpackages := nucliadb standalone reader search train writer ingest

test_target_prefix := test-
test_cov_target_prefix := test-cov-

test_subpackages_targets = $(addprefix $(test_target_prefix),$(subpackages))
test_cov_subpackages_targets = $(addprefix $(test_cov_target_prefix),$(subpackages))

pytest_flags := -s -rfE -v --tb=native
pytest_extra_flags :=

PYTEST := pytest $(pytest_flags) $(pytest_extra_flags)


.PHONY: check-system
check-system:
	../scripts/install-system-deps.sh

# Global format and lint are preferred in a single process as it's quite faster
# than executing each subpackage target

.PHONY: format
format:
	ruff check --fix --config=../ruff.toml .
	ruff format --config=../ruff.toml .


.PHONY: lint
lint:
	ruff check --config=../ruff.toml .
	ruff format --check --config=../ruff.toml .
	mypy --config-file=../mypy.ini .

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
		--cov=nucliadb \
		--cov=nucliadb_models \
		--cov=nucliadb_telemetry \
		--cov=nucliadb_utils \
		--cov-config=../.coveragerc \
		--cov-report xml \
		--cov-report term-missing:skip-covered \
		--cov-append \
		tests/$*
