#!/bin/bash
set -e

# parse partition number from hostname
export REPLICA_NUMBER=$(echo $HOSTNAME | grep -oE '[0-9]+$')

exec "$@"
