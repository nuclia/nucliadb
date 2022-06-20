#!/bin/bash
set -e

echo "DEBUG: Environment before = $(env)"

echo "DEBUG: Environment after = $(env)"

exec "$@"
