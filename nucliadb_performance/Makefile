reader_api=http://reader.nucliadb.svc.cluster.local:8080/api
search_api=http://search.nucliadb.svc.cluster.local:8080/api
error_tolerance=10
max_workers=100
duration_s=3600
ramp_up=300
sizing=--sizing
kbid=tiny
test=test-search-cluster

RED=\033[0;31m
NC=\033[0m

default: help

.PHONY: help
help: # Show help for each of the Makefile recipes.
	@echo "----------------------------------"
	@echo "NucliaDB Performance testing tool"
	@echo "----------------------------------\n"
	@echo "Available options:\n"
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | sort | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done
	@echo ""
	@echo "Example usages:\n"
	@echo "By default, tests take 10 minutes, but use the ${RED}duration_s${NC} param to change it.\nWorkers are added incrementally up to 100 or ${RED}max_workers${NC} param.\n"
	@echo "- To run with a fixed number of workers\n"
	@echo "    make test-search-cluster max_workers=100 sizing= ramp_up=0 duration_s=30\n"
	@echo "- To point to a local NucliaDB, use the ${RED}reader_api${NC} and ${RED}search_api${NC} params\n"
	@echo "    make test-search-cluster reader_api=http://localhost:8080/api search_api=http://localhost:8080/api\n"


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
test-search-cluster: # Stress test search on a cluster.
	echo " *  Test Started at: $$(date -u)"

	molotov ./nucliadb_performance/test_search_cluster.py $(sizing) --sizing-tolerance=$(error_tolerance) -w $(max_workers) --ramp-up $(ramp_up) -d $(duration_s) --force-shutdown

	echo " *  Test Finished at: $$(date -u)"


.PHONY: test-search-kb
test-search-kb: export SEARCH_API=$(search_api)
test-search-kb: export READER_API=$(reader_api)
test-search-kb: export KBID=$(kbid)
test-search-kb: # Stress test search on a KB
	echo " *  Test Started at: $$(date -u)"

	molotov ./nucliadb_performance/test_search_kb.py $(sizing) --sizing-tolerance=$(error_tolerance) -w $(max_workers) --ramp-up $(ramp_up) -d $(duration_s) --force-shutdown

	echo " *  Test Finished at: $$(date -u)"


pod=''
install-agent:  # Installs the nucliadb_performance package in a pod
	echo "Installing agent on ${pod}"

	kubectl cp -n nucliadb nucliadb_performance ${pod}:/usr/src/app/nucliadb_performance
	kubectl exec -qit -n nucliadb ${pod} -- make -C nucliadb_performance install


start-agent:  # Starts a test in an agent
	echo "Starting traffic"
	kubectl exec -qit -n nucliadb ${pod} -- make -C nucliadb_performance $(test) kbid=$(kbid) sizing=$(sizing) error_tolerance=$(error_tolerance) max_workers=$(max_workers) ramp_up=$(ramp_up) duration_s=$(duration_s) &


stop-agent:  # Stops a running test in an agent
	echo "Stopping ${pod}"
	kubectl exec -qit -n nucliadb ${pod} -- apt-get install psmisc && killall -s 9 molotov
