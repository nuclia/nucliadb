#!/bin/bash

set -e

BUILD_SERVER_URL="https://ci.stashify.tech/rust-build-server"
# BUILD_SERVER_URL="http://localhost:8000"

if [ -z "$BRANCH" ]; then
    echo "BRANCH is not set"
    exit 1
fi
if [ -z "$COMMIT_HASH" ]; then
    echo "COMMIT_HASH is not set"
    exit 1
fi
if [ -z "$SECRET_KEY" ]; then
    echo "SECRET_KEY is not set"
    exit 1
fi

TEST=${TEST:-false}
RELEASE=${RELEASE:-false}
MATURIN=${MATURIN:-false}

echo "Building: $BRANCH $COMMIT_HASH -- test: $TEST"

# JSON data
json_data=$(curl -f "$BUILD_SERVER_URL/build" \
    --retry 5 \
    --retry-all-errors \
    --retry-delay 1 \
    -H "X-Secret-Key:$SECRET_KEY" \
    -H 'content-type: application/json' \
    --data "{\"git_url\": \"https://github.com/nuclia/nucliadb.git\",\"branch\": \"$BRANCH\",\"commit_hash\": \"$COMMIT_HASH\",\"release\": $RELEASE, \"test\": $TEST, \"maturin\": $MATURIN, \"cargo_file\": \"nucliadb_node_binding/Cargo.toml\"}")

echo "JSON data:"
echo $json_data

# Parse JSON and download files
binaries=$(echo "$json_data" | jq -r '.binaries[]')
test_binaries=$(echo "$json_data" | jq -r '.test_binaries[]')
wheels=$(echo "$json_data" | jq -r '.wheels[]')
base_url="$BUILD_SERVER_URL/download-binary/$COMMIT_HASH"

mkdir -p builds/tests
mkdir -p builds/wheels

if [ "$MATURIN" = "true" ]; then
    for wheel in $wheels; do
        url="${base_url}/${wheel}"
        echo "Downloading $wheel from $url"
        curl -f --retry 3 --retry-delay 2 -o "builds/wheels/$wheel" "$url" -H "X-Secret-Key:$SECRET_KEY"
    done
fi

for binary in $binaries; do
    url="${base_url}/${binary}"
    echo "Downloading $binary from $url"
    curl -f --retry 3 --retry-delay 2 -o "builds/$binary" "$url" -H "X-Secret-Key:$SECRET_KEY"
done

if [ "$TEST" = "true" ]; then
    for binary in $test_binaries; do
        url="${base_url}/${binary}"
        echo "Downloading $binary from $url"
        curl -f --retry 3 --retry-delay 2 -o "builds/tests/$binary" "$url" -H "X-Secret-Key:$SECRET_KEY"
    done
fi