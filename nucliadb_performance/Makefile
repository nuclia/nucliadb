reader_api=http://reader.nucliadb.svc.cluster.local:8080/api
search_api=http://search.nucliadb.svc.cluster.local:8080/api
error_tolerance=5
max_workers=10
duration_s=60
ramp_up=10
sizing=--sizing
kbid=tiny
kb_slug=tiny
test=test-search-cluster
benchmark_output=benchmark_output.json


BOLD_GREEN:=\033[1;32m
NO_COLOR:=\033[00m

default: help

.PHONY: help
help: # Show help for each of the Makefile recipes.
	@echo "----------------------------------"
	@echo "NucliaDB Performance testing tool"
	@echo "----------------------------------\n"
	@echo "Available options:\n"
	@grep -E '^[a-zA-Z0-9 -]+:.*#' Makefile | sort | while read -r l; do printf "$(BOLD_GREEN)$$(echo $$l | cut -f 1 -d':')$(NO_COLOR):$$(echo $$l | cut -f 2- -d'#')\n"; done
	@echo ""
	@echo "Example usages:\n"
	@echo "- Run tests against a local nucliadb at port 8080:\n"
	@echo "    make run kb_slug=tiny max_workers=4 duration_s=20\n"


.PHONY: install
install: # Install the package
	pip install --upgrade pip wheel
	pip install -e .


.PHONY: install-dev
install-dev: # Install the package for development
	pip install --upgrade pip wheel
	pip install -r ../test-requirements.txt -r ../code-requirements.txt -r requirements.txt
	pip install -e .


.PHONY: format
format: # Format the code
	cd .. && isort --profile black nucliadb_performance
	black .

.PHONY: lint
lint: # Run lint checks
	flake8 nucliadb_performance --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_performance
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini . --show-traceback


.PHONY: test-search-cluster
test-search-cluster: export SEARCH_API=$(search_api)
test-search-cluster: export READER_API=$(reader_api)
test-search-cluster:
	echo " *  Test Started at: $$(date -u)"

	molotov ./nucliadb_performance/hosted/test_search_cluster.py $(sizing) --sizing-tolerance=$(error_tolerance) -w $(max_workers) --ramp-up $(ramp_up) -d $(duration_s) --force-shutdown

	echo " *  Test Finished at: $$(date -u)"


.PHONY: test-search-kb
test-search-kb: export SEARCH_API=$(search_api)
test-search-kb: export READER_API=$(reader_api)
test-search-kb: export KBID=$(kbid)
test-search-kb:
	echo " *  Test Started at: $$(date -u)"

	molotov ./nucliadb_performance/hosted/test_search_kb.py $(sizing) --sizing-tolerance=$(error_tolerance) -w $(max_workers) --ramp-up $(ramp_up) -d $(duration_s) --force-shutdown

	echo " *  Test Finished at: $$(date -u)"


.PHONY: test-standalone-search
test-standalone-search: export READER_API=http://localhost:8080/api
test-standalone-search: export SEARCH_API=http://localhost:8080/api
test-standalone-search: export KB_SLUG=$(kb_slug)
test-standalone-search: export BENCHMARK_OUTPUT=$(benchmark_output)
test-standalone-search: export SAVED_REQUESTS_FILE=standalone_search_requests.json
test-standalone-search:	download-test-data # Stress test search on a standalone instance.
	echo " *  Test Started at: $$(date -u)"

	molotov ./nucliadb_performance/standalone/test_search.py -w $(max_workers) -d $(duration_s) --force-shutdown

	echo " *  Test Finished at: $$(date -u)"


.PHONY: download-test-data
download-test-data: # Download the test data for standalone search
	@mkdir -p exports
	@if [ -f exports/$(kb_slug).export ]; then \
		echo "File exports/$(kb_slug).export already exists, skipping download"; \
	else \
		echo "Downloading $(kb_slug).export"; \
		wget https://storage.googleapis.com/nucliadb_indexer/nucliadb_performance/exports/$(kb_slug).export -q -O exports/$(kb_slug).export --show-progress; \
	fi; \


run:	test-standalone-search # Run the performance tests