#!/bin/bash
MAX_RETRIES=3
MAX_TIMEOUT=60
COMMAND="nc -zv 127.0.0.1 2379"

unameOut="$(uname -s)"
case "${unameOut}" in
Linux*) machine=Linux ;;
Darwin*) machine=Mac ;;
CYGWIN*) machine=Cygwin ;;
MINGW*) machine=MinGw ;;
*) machine="UNKNOWN:${unameOut}" ;;
esac

INSTALL_SCRIPT_PATH="./install.sh"
TIUP_BIN="$HOME/.tiup/bin/tiup"

start_tikv() {
	# tiup installation
	if ! [ -e $TIUP_BIN ]; then
		if ! [ -e $INSTALL_SCRIPT_PATH ]; then
			echo $INSTALL_SCRIPT_PATH
			curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh -o $INSTALL_SCRIPT_PATH
			chmod +x $INSTALL_SCRIPT_PATH
		fi

		sh $INSTALL_SCRIPT_PATH

		if [ "$machine" = "Mac" ] && [ -f "$HOME/.bash_profile" ]; then
			source "$HOME/.bash_profile"
		elif [ -f "$HOME/.profile" ]; then
			source "$HOME/.profile"
		else
			echo "Error: Neither .bash_profile nor .profile found."
		fi

		# we synchronously install all required components here, so `tiup playground` does not.
		tiup install playground
		tiup install tikv
		tiup install pd
	fi

	tiup playground --mode tikv-slim --without-monitor &

	start_time=$(date +%s)

	while true; do
		$COMMAND
		exit_code=$?
		current_time=$(date +%s)
		elapsed_time=$((current_time - start_time))

		if [ $exit_code -eq 0 ]; then
			echo "Running on 127.0.0.1 2379."
			sleep 5 # breathe
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
