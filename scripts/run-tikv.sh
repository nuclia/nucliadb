#!/bin/bash
set -ex

MAX_RETRIES=3
MAX_TIMEOUT=60
COMMAND="nc -zv 127.0.0.1 2379"

start_tikv() {
	curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh -s -- v1.11.3
	source /home/runner/.profile
	tiup playground 5.3.1 --mode tikv-slim --without-monitor &

	start_time=$(date +%s)

	while true; do
		$COMMAND
		exit_code=$?
		current_time=$(date +%s)
		elapsed_time=$((current_time - start_time))

		if [ $exit_code -eq 0 ]; then
			echo "Running on 127.0.0.1 2379."
			break
		elif [ $elapsed_time -ge $MAX_TIMEOUT ]; then
			echo "Not running within the maximum timeout of $MAX_TIMEOUT seconds."
			return 1
			break
		else
			echo "Waiting to come up..."
			sleep 1 # You can adjust the sleep time as needed
		fi
	done

	return 0
}

retry() {
	local command="$1"
	local retries=$MAX_RETRIES
	local exit_code=1 # Default exit code (failure)

	while [ $retries -gt 0 ]; do
		$command
		exit_code=$?

		# If the command succeeds (exit code 0), return its exit code
		if [ $exit_code -eq 0 ]; then
			return $exit_code
		fi

		# If the command fails, decrement the retry count
		((retries--))

		if [ $retries -gt 0 ]; then
			echo "Retrying $command ($retries retries left)..."
		else
			echo "Max retries reached for $command"
			return $exit_code # Return the exit code if max retries are reached
		fi

		# delay before next retry
		sleep 5
	done

	return $exit_code # Return the exit code after all retry attempts
}

# Call the retry function with the start_tikv function and store the exit code
retry start_tikv
exit_code=$?

exit $exit_code
