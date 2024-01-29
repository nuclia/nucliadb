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
    $SUDO dnf update -y
    $SUDO dnf install -y gcc make zlib-devel \
      ncurses-devel nss-devel openssl-devel \
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

# Install Python
$SUDO mkdir -p /opt/python
cd /opt/python
$SUDO wget https://www.python.org/ftp/python/$PYTHON_VERSION/Python-$PYTHON_VERSION.tgz
$SUDO tar -xf Python-$PYTHON_VERSION.tgz
cd Python-$PYTHON_VERSION
$SUDO ./configure --enable-optimizations
$SUDO make -j 2
$SUDO make install

# Install NucliaDB
$SUDO /opt/python/Python-$PYTHON_VERSION/python -m venv $INSTALL_DIR
cd $INSTALL_DIR
$SUDO $INSTALL_DIR/bin/python -m pip install --upgrade pip
$SUDO $INSTALL_DIR/bin/pip install nucliadb

$SUDO ln -s $INSTALL_DIR/bin/nucliadb /bin/nucliadb

# Install systemd unit
cat << EOF | $SUDO tee /usr/lib/systemd/system/nucliadb.service > /dev/null
[Unit]
Description=NucliaDB
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=nucliadb
Group=nucliadb
EnvironmentFile=-/etc/default/nucliadb
ExecStart=/usr/bin/nucliadb
WorkingDirectory=/var/lib/nucliadb

[Install]
WantedBy=multi-user.target
EOF

# Install config
cat << EOF | $SUDO tee /etc/default/nucliadb > /dev/null
LOG_OUTPUT_TYPE=STDOUT

DRIVER=pg
DRIVER_PG_URL=

NUA_API_KEY=
EOF

# Create user and working directory
$SUDO groupadd -r nucliadb
$SUDO useradd -M -r -g nucliadb nucliadb
$SUDO mkdir /var/lib/nucliadb
$SUDO chown nucliadb:nucliadb /var/lib/nucliadb

# Install upgrade script
cat << EOF | $SUDO tee /usr/bin/upgrade-nucliadb > /dev/null
#!/bin/bash
#
# The purpose of this script is to be used to upgrade NucliaDB to the latest
# version. Works if it has been installed using the install-vm.sh script.
#
set -e
INSTALL_DIR=/opt/nucliadb

SUDO=''
if (($EUID != 0)); then
  SUDO='sudo'
fi

$SUDO systemctl stop nucliadb
$SUDO $INSTALL_DIR/bin/pip install --upgrade nucliadb
$SUDO systemctl start nucliadb
EOF

$SUDO chmod +x /usr/bin/upgrade-nucliadb

# Install config
cat << EOF | $SUDO tee /etc/default/nucliadb > /dev/null
LOG_OUTPUT_TYPE=STDOUT

DRIVER=pg
DRIVER_PG_URL=

NUA_API_KEY=
EOF