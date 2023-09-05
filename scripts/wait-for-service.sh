#!/bin/bash

MAX_TIMEOUT=90
COMMAND="nc -zv 127.0.0.1 $1"

start_time=$(date +%s)
while true; do
    $COMMAND
    exit_code=$?
    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))

    if [ $exit_code -eq 0 ]; then
        echo "Running on 127.0.0.1 $1."
        break
    elif [ $elapsed_time -ge $MAX_TIMEOUT ]; then
        echo "Not runing within the maximum timeout of $MAX_TIMEOUT seconds."
        exit 1
        break
    else
        echo "Waiting to come up..."
        sleep 1  # You can adjust the sleep time as needed
    fi
done