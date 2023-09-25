#!/bin/bash
#
# The purpose of this script is to be used to install NucliaDB manually on a
# Debian based system.
#
set -e

PYTHON_VERSION=3.11.4
INSTALL_DIR=/opt/nucliadb

SUDO=''
if (($EUID != 0)); then
  SUDO='sudo'
fi

if [ ! -f /etc/redhat-release ] && [ ! -f /etc/debian_version ]; then
    echo "Unsupported OS Type"
    exit 1
fi

# Check if /etc/redhat-release exists (Red Hat/Fedora)
if [ -f /etc/redhat-release ]; then
    echo "Detected Red Hat or Fedora"
    $SUDO dnf install -y gcc make zlib-devel \
      ncurses-devel gdbm-devel nss-devel openssl-devel \
      readline-devel libffi-devel sqlite-devel wget \
      bzip2-devel xz-devel tk-devel
fi

# Check if /etc/debian_version exists (Debian/Ubuntu)
if [ -f /etc/debian_version ]; then
    echo "Detected Debian or Ubuntu. Installing system dependencies."
    $SUDO apt update
    $SUDO apt install -y build-essential zlib1g-dev libncurses5-dev \
      libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev \
      libsqlite3-dev wget libbz2-dev liblzma-dev tk8.6-dev
fi



$SUDO mkdir -p /opt/python
cd /opt/python
$SUDO wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
$SUDO tar -xf Python-$PYTHON_VERSION.tgz
cd Python-$PYTHON_VERSION
$SUDO ./configure --enable-optimizations
$SUDO make -j 2
$SUDO make install

$SUDO useradd --create-home --shell /bin/bash nucliadb

AS_NDB_USER=''
if (($EUID != 0)); then
  AS_NDB_USER='sudo -u nucliadb'
fi

$SUDO /opt/python/Python-$PYTHON_VERSION/python -m venv $INSTALL_DIR
$SUDO chown -R nucliadb:nucliadb $INSTALL_DIR
cd $INSTALL_DIR
$AS_NDB_USER $INSTALL_DIR/bin/python -m pip install --upgrade pip
$AS_NDB_USER $INSTALL_DIR/bin/pip install nucliadb

$SUDO ln -s $INSTALL_DIR/bin/nucliadb /bin/nucliadb