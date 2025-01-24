#!/bin/bash
set -ex

UNAME="$(uname -s)"
case "${UNAME}" in
Linux*) machine=linux ;;
Darwin*) machine=macos ;;
CYGWIN*) machine=cygwin ;;
MINGW*) machine=mingw ;;
*) machine="UNKNOWN:${UNAME}" ;;
esac

if [ "$machine" == "linux" ]; then
	SUDO=''
	if (($EUID != 0)); then
		SUDO='sudo'
	fi

	source /etc/os-release
	case $ID in
	debian | ubuntu | mint)
		$SUDO apt-get -y update || true
		$SUDO apt-get install -y libdw-dev pkg-config libssl-dev cpanminus
		$SUDO cpanm IPC::Cmd
		;;

	fedora | rhel | centos | almalinux)
		$SUDO yum update -y
		$SUDO yum -y install elfutils-devel pkgconfig openssl-devel perl-IPC-Cmd protobuf-compiler
		;;

	*)
		echo -n "unsupported linux distro"
		;;
	esac
fi

if [ "$machine" == "macos" ]; then
	brew install protobuf
fi
