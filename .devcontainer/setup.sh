#!/bin/bash
# Update apt and install necessary packages
apt update -y
apt install -y python3-dev libpq-dev cargo

# Download and install UV
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install 30.2 protoc version
PB_REL="https://github.com/protocolbuffers/protobuf/releases"
curl -LO $PB_REL/download/v30.2/protoc-30.2-linux-x86_64.zip
unzip protoc-30.2-linux-x86_64.zip -d $HOME/.local
export PATH="$PATH:$HOME/.local/bin"
