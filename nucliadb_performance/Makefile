.PHONY: install
install:
	pip install --upgrade pip wheel
	pip install -e .


.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	pip install -r ../test-requirements.txt -r ../code-requirements.txt -r requirements.txt
	pip install -e .


.PHONY: format
format:
	cd .. && isort --profile black nucliadb_performance
	black .

.PHONY: lint
lint:
	flake8 nucliadb_performance --config=setup.cfg
	cd .. && isort -c --profile black nucliadb_performance
	black --check .
	MYPYPATH=../mypy_stubs mypy --config-file=../mypy.ini . --show-traceback

max_workers=100
duration=600
ramp_up=500
sizing="--sizing"


.PHONY: test-search-cluster
test-search-cluster: export READER_API=http://reader.nucliadb.svc.cluster.local:8080/api
test-search-cluster: export SEARCH_API=http://search.nucliadb.svc.cluster.local:8080/api
test-search-cluster:
	echo " *  Test Started at: $$(date -u)"

	molotov ./nucliadb_performance/test_search_cluster.py $(sizing) --sizing-tolerance=10 -w $(max_workers) --ramp-up $(ramp_up) -d $(duration) --force-shutdown

	echo " *  Test Finished at: $$(date -u)"


.PHONY: test-search-kb
test-search-kb: export READER_API=http://reader.nucliadb.svc.cluster.local:8080/api
test-search-kb: export SEARCH_API=http://search.nucliadb.svc.cluster.local:8080/api
test-search-kb:
	echo " *  Test Started at: $$(date -u)"

	molotov ./nucliadb_performance/test_search_kb.py $(sizing) --sizing-tolerance 10 --ramp-up $(ramp_up) -d $(duration) --force-shutdown

	echo " *  Test Finished at: $$(date -u)"