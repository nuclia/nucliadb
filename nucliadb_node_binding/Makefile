.PHONY: install-dev
install-dev:
	pip install --upgrade pip wheel
	cd .. && pip install \
		-r test-requirements.txt \
		-r code-requirements.txt
	pip install -e .

.PHONY: test
test:
	pytest -s --tb=native -v test.py

.PHONY: build
build:
	maturin build
