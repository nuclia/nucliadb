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
if [ -z "$secret_key" ]; then
    echo "secret_key is not set"
    exit 1
fi

TEST=${TEST:-false}

echo "Building: $BRANCH $COMMIT_HASH -- test: $TEST"

# JSON data
json_data=$(curl -f "$BUILD_SERVER_URL/build" \
    -H "X-Secret-Key:$secret_key" \
    -H 'content-type: application/json' \
    --data "{\"git_url\": \"https://github.com/nuclia/nucliadb.git\",\"branch\": \"$BRANCH\",\"commit_hash\": \"$COMMIT_HASH\",\"release\": false, \"test\": $TEST}")

echo "JSON data:"
echo $json_data

# Parse JSON and download files
binaries=$(echo "$json_data" | jq -r '.binaries[]')
test_binaries=$(echo "$json_data" | jq -r '.test_binaries[]')
base_url="$BUILD_SERVER_URL/download-binary/$COMMIT_HASH"

mkdir -p http-builds/tests

for binary in $binaries; do
    url="${base_url}/${binary}"
    echo "Downloading $binary from $url"
    curl -f -o "http-builds/$binary" "$url" -H "X-Secret-Key:$secret_key"
done

if [ "$TEST" = "true" ]; then
    for binary in $test_binaries; do
        url="${base_url}/${binary}"
        echo "Downloading $binary from $url"
        curl -f -o "http-builds/tests/$binary" "$url" -H "X-Secret-Key:$secret_key"
    done
fi