#!/bin/bash
set -e

# remove self from swim peers list
export SWIM_PEERS_ADDR=$(echo $SWIM_PEERS_ADDR | sed -re "s|\"$HOSTNAME\.([a-zA-Z0-9]+\.?)+(:[0-9]+)?\",?||g" | sed -re "s|,\s*]|]|g")

# parse partition number from hostname
export REPLICA_NUMBER=$(echo $HOSTNAME | grep -oE '[0-9]+$')

exec "$@"
