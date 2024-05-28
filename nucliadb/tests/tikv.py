# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
import os
import platform
import signal
import subprocess
import tarfile
import tempfile
import time
from io import BytesIO

import pytest
import requests
from tikv_client import TransactionClient  # type: ignore


class TiKVd(object):
    def __init__(
        self,
        port=20160,
        pd_port=2379,
        peer_port=2380,
        tikv_bin_name="tikv-server",
        pd_bin_name="pd-server",
        host="127.0.0.1",
        path="",
        debug=False,
    ):
        self.port = port
        self.pd_port = pd_port
        self.peer_port = peer_port
        self.tikv_bin_name = tikv_bin_name
        self.pd_bin_name = pd_bin_name
        self.path = path
        self.host = host
        self.tmpfolder = None
        self.debug = debug
        self.proc = None
        self.proc2 = None

    def start(self):
        self.tmpfolder = tempfile.TemporaryDirectory()

        cmd = [
            f"{self.path}/{self.tikv_bin_name}",
            f"--pd-endpoints={self.host}:{self.pd_port}",
            f"--addr={self.host}:{self.port}",
            f"--data-dir={self.tmpfolder.name}/tikv1",
            f"--log-file={self.tmpfolder.name}/tikv1.log",
        ]

        if self.debug:
            self.proc2 = subprocess.Popen(cmd)
        else:
            self.proc2 = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

        if self.debug:
            if self.proc2 is None:
                print(
                    "[\031[0;33mDEBUG\033[0;0m] Failed to start server listening on port %d started."
                    % self.port
                )
            else:
                print(
                    "[\033[0;33mDEBUG\033[0;0m] Server listening on port %d started."
                    % self.port
                )
        cmd = [
            f"{self.path}/{self.pd_bin_name}",
            "--name=pd",
            f"--data-dir={self.tmpfolder.name}",
            f"--client-urls=http://{self.host}:{self.pd_port}",
            f"--peer-urls=http://{self.host}:{self.peer_port}",
            f"--initial-cluster=pd=http://{self.host}:{self.peer_port}",
        ]

        if self.debug:
            self.proc = subprocess.Popen(cmd)
        else:
            self.proc = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

        if self.debug:
            if self.proc is None:
                print(
                    "[\031[0;33mDEBUG\033[0;0m] Failed to start server listening on port %d started."
                    % self.pd_port
                )
            else:
                print(
                    "[\033[0;33mDEBUG\033[0;0m] Server listening on port %d started."
                    % self.pd_port
                )
        return self.proc

    def stop(self):
        if self.debug:
            print(
                "[\033[0;33mDEBUG\033[0;0m] Server listening on %d will stop."
                % self.port
            )

        if self.debug:
            if self.proc is None:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Failed terminating server listening on port %d"
                    % self.port
                )
            if self.proc2 is None:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Failed terminating server listening on port %d"
                    % self.port
                )

        if self.proc.returncode is not None:
            if self.debug:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Server listening on port {port} finished running already with exit {ret}".format(  # noqa
                        port=self.pd_port, ret=self.proc.returncode
                    )
                )
        elif self.proc2.returncode is not None:
            if self.debug:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Server listening on port {port} finished running already with exit {ret}".format(  # noqa
                        port=self.port, ret=self.proc2.returncode
                    )
                )
        else:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
            os.kill(self.proc2.pid, signal.SIGKILL)
            self.proc2.wait()
            if self.debug:
                print(
                    "[\033[0;33mDEBUG\033[0;0m] Server listening on %d was stopped."
                    % self.port
                )
        if self.tmpfolder is not None:
            self.tmpfolder.cleanup()
            self.tmpfolder = None


def start_tikvd(tikvd: TiKVd):
    tikvd.start()

    endpoint = "127.0.0.1:{port}".format(port=tikvd.port)
    retries = 0
    while True:
        if retries > 100:
            break

        try:
            connection = TransactionClient.connect(endpoint)
            txn = connection.begin(pessimistic=True)
            txn.rollback()
        except:  # noqa
            retries += 1
            time.sleep(0.1)


@pytest.fixture(scope="session")
def tikvd():
    if os.environ.get("TESTING_TIKV_LOCAL", None):
        yield "localhost", "XX", "2379"
        return

    if not os.path.isfile("tikv-server"):
        version = "v5.3.1"
        arch = platform.machine()
        if arch == "x86_64":
            arch = "amd64"
        system = platform.system().lower()

        resp = requests.get(
            f"https://tiup-mirrors.pingcap.com/tikv-{version}-{system}-{arch}.tar.gz"
        )

        zipfile = tarfile.open(fileobj=BytesIO(resp.content), mode="r:gz")

        zipfile.extract(f"tikv-server")
        os.chmod("tikv-server", 755)

    if not os.path.isfile("pd-server"):
        version = "v5.3.1"
        arch = platform.machine()
        if arch == "x86_64":
            arch = "amd64"
        system = platform.system().lower()

        resp = requests.get(
            f"https://tiup-mirrors.pingcap.com/pd-{version}-{system}-{arch}.tar.gz"
        )

        zipfile = tarfile.open(fileobj=BytesIO(resp.content), mode="r:gz")

        zipfile.extract(f"pd-server")
        os.chmod("pd-server", 755)

    server = TiKVd(debug=True)
    server.tikv_bin_name = "tikv-server"
    server.pd_bin_name = "pd-server"
    server.path = os.getcwd()

    start_tikvd(server)
    print("Started TiKVd")

    for i in range(100):
        resp = requests.get(f"http://{server.host}:{server.pd_port}/pd/api/v1/stores")
        if (
            resp.status_code == 200
            and resp.json()["stores"][0]["store"]["state_name"] == "Up"
        ):
            break
        print(resp.status_code)
        print(resp.json())
        time.sleep(1)

    yield server.host, server.port, server.pd_port
    server.stop()
