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


.PHONY: test-search-cluster
test-search-cluster:
	echo "Test started at $(shell date)"
	molotov ./nucliadb_performance/test_search_cluster.py --sizing --sizing-tolerance=10 -w 100 --ramp-up 500 -d 600
	echo "Test finished at $(shell date)"