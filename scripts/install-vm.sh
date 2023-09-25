#!/bin/bash
#
# The purpose of this script is to be used to install NucliaDB manually on a
# Debian based system.
#
set -e

PYTHON_VERSION=3.11.4

sudo apt update
sudo apt install -y build-essential zlib1g-dev libncurses5-dev \
  libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev \
  libsqlite3-dev wget libbz2-dev liblzma-dev tk8.6-dev

wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
tar -xf Python-$PYTHON_VERSION.tgz
cd Python-$PYTHON_VERSION
./configure --enable-optimizations
make -j 2
sudo make install
cd ..

python3 -m venv nucliadb
cd nucliadb
source bin/activate
python -m pip install --upgrade pip
pip install nucliadb
