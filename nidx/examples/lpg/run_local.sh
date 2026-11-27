#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../../.."

data_dir="${NIDX_LPG_DATA_DIR:-$HOME/nidx-lpg}"
mkdir -p "$data_dir/input" "$data_dir/segments"

export SQLX_OFFLINE="${SQLX_OFFLINE:-true}"
export METADATA__DATABASE_URL="${METADATA__DATABASE_URL:-postgresql://postgres@127.0.0.1:55432/postgres}"
export INDEXER__OBJECT_STORE=file
export INDEXER__FILE_PATH="$data_dir/input"
export STORAGE__OBJECT_STORE=file
export STORAGE__FILE_PATH="$data_dir/segments"
export LPG__INDEXER_ADDRESS=http://127.0.0.1:10002
export LPG__SEARCHER_ADDRESS=http://127.0.0.1:10001

exec cargo run --release --manifest-path nidx/Cargo.toml --bin nidx -- indexer searcher api scheduler worker
