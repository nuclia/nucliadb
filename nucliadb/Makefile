include ../nucliadb.mk

subpackages := nucliadb standalone reader search train writer ingest

test_target_prefix := test-
test_cov_target_prefix := test-cov-

test_subpackages_targets = $(addprefix $(test_target_prefix),$(subpackages))
test_cov_subpackages_targets = $(addprefix $(test_cov_target_prefix),$(subpackages))

.PHONY: check-system
check-system:
	../scripts/install-system-deps.sh

# Global format and lint are preferred in a single process as it's quite faster
# than executing each subpackage target

.PHONY: format
format:
	ruff check --fix --config=../ruff.toml $(CODE_PATHS)
	ruff format --config=../ruff.toml $(CODE_PATHS)


.PHONY: lint
lint:
	ruff check --config=../ruff.toml $(CODE_PATHS)
	ruff format --check --config=../ruff.toml $(CODE_PATHS)
	ty check $(CODE_PATHS)
	MYPYPATH=../nidx mypy --config-file=../mypy.ini $(CODE_PATHS)

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
		$(pytest_cov_report_flags) \
		--cov=nucliadb \
		--cov=nucliadb_models \
		--cov=nucliadb_telemetry \
		--cov=nucliadb_utils \
		--cov-config=../.coveragerc \
		--cov-append \
		tests/$*
