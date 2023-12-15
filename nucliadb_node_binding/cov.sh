#!/usr/bin/env bash
set -ex

cargo llvm-cov show-env --export-prefix >.env
cargo llvm-cov clean --workspace
rm -rf ../target/wheels/nucliadb_node_binding*
source .env

maturin build
pip install ../target/wheels/nucliadb_node_binding*.whl

pytest -rfE --cov=nucliadb_node_binding --cov-config=../.coveragerc -s --tb=native -v --cov-report term-missing:skip-covered --cov-report xml tests

# generating coverage for the CI
cargo llvm-cov report --ignore-filename-regex="^(.*/nucliadb_vectors/).*|^(.*/nucliadb_texts/).*|^(.*/nucliadb_relations/).*|^(.*/nucliadb_core/).*|^(.*/nucliadb_node/).*|^(.*/nucliadb_paragraphs/).*|^(.*/nucliadb_protos/)|^(.*/nucliadb/nucliadb/)" --lcov --output-path coverage.lcov

# coverage for the console
cargo llvm-cov report --ignore-filename-regex="^(.*/nucliadb_vectors/).*|^(.*/nucliadb_texts/).*|^(.*/nucliadb_relations/).*|^(.*/nucliadb_core/).*|^(.*/nucliadb_node/).*|^(.*/nucliadb_paragraphs/).*|^(.*/nucliadb_protos/)|^(.*/nucliadb/nucliadb/)"
