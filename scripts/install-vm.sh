#!/bin/bash
#
# The purpose of this script is to be used to install NucliaDB manually on a
# Debian based system.
#
set -e

sudo apt update
sudo apt install -y build-essential zlib1g-dev libncurses5-dev \
  libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev \
  libsqlite3-dev wget libbz2-dev liblzma-dev tk8.6-dev

wget https://www.python.org/ftp/python/3.9.17/Python-3.9.17.tgz
tar -xf Python-3.9.17.tgz
cd Python-3.9.17
./configure --enable-optimizations
make -j 2
sudo make install
cd ..

python3 -m venv nucliadb
cd nucliadb
source bin/activate
python -m pip install --upgrade pip
pip install nucliadb