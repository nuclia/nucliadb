#!/bin/bash
set -ex

mkdir protoc
cd protoc
curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v29.3/protoc-29.3-linux-x86_64.zip
unzip protoc-29.3-linux-x86_64.zip
cp -r bin include /usr
