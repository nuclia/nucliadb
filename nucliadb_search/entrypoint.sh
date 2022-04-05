#!/bin/bash
set -e

echo "DEBUG: Environment before = $(env)"

# remove self from swim peers list
export SWIM_PEERS_ADDR=$(echo $SWIM_PEERS_ADDR | sed -re "s|\"$HOSTNAME\.([a-zA-Z0-9]+\.?)+(:[0-9]+)?\",?||g" | sed -re "s|,\s*]|]|g")

echo "DEBUG: Environment after = $(env)"

exec "$@"
