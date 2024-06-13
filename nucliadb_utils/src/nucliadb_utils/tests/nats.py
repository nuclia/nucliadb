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

import http.client
import os
import platform
import signal
import socket
import subprocess
import tempfile
import time
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import pytest
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore


class Gnatsd(object):
    def __init__(
        self,
        port=4222,
        user="",
        password="",
        token="",
        timeout=0,
        http_port=8222,
        debug=False,
        tls=False,
        cluster_listen=None,
        routes=None,
        config_file=None,
    ):  # pragma: no cover
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.http_port = http_port
        self.proc = None
        self.debug = debug
        self.tls = tls
        self.token = token
        self.cluster_listen = cluster_listen
        self.routes = routes or []
        self.bin_name = "gnatsd"
        self.config_file = config_file
        self.folder = None

        env_debug_flag = os.environ.get("DEBUG_NATS_TEST")
        if env_debug_flag == "true":
            self.debug = True

    def start(self):
        folder = tempfile.TemporaryDirectory()
        cmd = [
            f"{self.path}/{self.bin_name}",
            "-js",
            "-p",
            "%d" % self.port,
            "-m",
            "%d" % self.http_port,
            "-a",
            "0.0.0.0",
            "-sd",
            folder.name,
            "--log",
            f"{folder.name}/log.log",
        ]
        self.folder = folder
        if self.user != "":
            cmd.append("--user")
            cmd.append(self.user)
        if self.password != "":
            cmd.append("--pass")
            cmd.append(self.password)

        if self.token != "":
            cmd.append("--auth")
            cmd.append(self.token)

        if self.debug:
            cmd.append("-DV")

        if self.tls:
            cmd.append("--tls")
            cmd.append("--tlscert")
            cmd.append("tests/certs/server-cert.pem")
            cmd.append("--tlskey")
            cmd.append("tests/certs/server-key.pem")
            cmd.append("--tlsverify")
            cmd.append("--tlscacert")
            cmd.append("tests/certs/ca.pem")

        if self.cluster_listen is not None:
            cmd.append("--cluster_listen")
            cmd.append(self.cluster_listen)

        if len(self.routes) > 0:
            cmd.append("--routes")
            cmd.append(",".join(self.routes))

        if self.config_file is not None:
            cmd.append("--config")
            cmd.append(self.config_file)

        if self.debug:
            self.proc = subprocess.Popen(cmd)
        else:
            self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        if self.debug:
            if self.proc is None:
                print(
                    "[\031[0;33mDEBUG\033[0;0m] Failed to start server listening on port %d started."
                    % self.port
                )
            else:
                print("[\033[0;33mDEBUG\033[0;0m] Server listening on port %d started." % self.port)
        return self.proc

    def stop(self):
        if self.folder is not None:
            self.folder.cleanup()
        if self.debug:
            print("[\033[0;33mDEBUG\033[0;0m] Server listening on %d will stop." % self.port)

        if self.debug:
            if self.proc is None:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Failed terminating server listening on port %d"
                    % self.port
                )

        if self.proc.returncode is not None:
            if self.debug:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Server listening on port {port} finished running already with exit {ret}".format(  # noqa: E501
                        port=self.port, ret=self.proc.returncode
                    )
                )
        else:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
            if self.debug:
                print("[\033[0;33mDEBUG\033[0;0m] Server listening on %d was stopped." % self.port)


class NatsServer(Gnatsd):  # pragma: no cover
    def __init__(self):
        super(Gnatsd, self)
        self.bin_name = "nats-server"


def start_gnatsd(gnatsd: Gnatsd):  # pragma: no cover
    gnatsd.start()

    endpoint = "127.0.0.1:{port}".format(port=gnatsd.http_port)
    retries = 0
    while True:
        if retries > 100:
            break

        try:
            httpclient = http.client.HTTPConnection(endpoint, timeout=5)
            httpclient.request("GET", "/varz")
            response = httpclient.getresponse()
            if response.status == 200:
                break
        except Exception:
            retries += 1
            time.sleep(0.1)


@pytest.fixture(scope="session")
def natsd_server():  # pragma: no cover
    if not os.path.isfile("nats-server"):
        version = "v2.10.12"
        arch = platform.machine()
        if arch == "x86_64":
            arch = "amd64"
        system = platform.system().lower()

        url = f"https://github.com/nats-io/nats-server/releases/download/{version}/nats-server-{version}-{system}-{arch}.zip"

        resp = urlopen(url)
        zipfile = ZipFile(BytesIO(resp.read()))

        file = zipfile.open(f"nats-server-{version}-{system}-{arch}/nats-server")
        content = file.read()
        with open("nats-server", "wb") as f:
            f.write(content)
        os.chmod("nats-server", 755)

    server = Gnatsd(port=4222)
    server.bin_name = "nats-server"
    server.path = os.getcwd()
    return server


@pytest.fixture(scope="session")
def natsd(natsd_server: Gnatsd):  # pragma: no cover
    start_gnatsd(natsd_server)
    print("Started natsd")
    yield f"nats://localhost:{natsd_server.port}"
    natsd_server.stop()


images.settings["nats"] = {
    "image": "nats",
    "version": "2.10.12",
    "options": {"command": ["-js"], "ports": {"4222": None}},
}


class NatsImage(BaseImage):  # pragma: no cover
    name = "nats"
    port = 4222

    def check(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((self.host, int(self.get_port())))
            return True
        except Exception:
            return False


nats_image = NatsImage()


@pytest.fixture(scope="session")
def natsdocker():  # pragma: no cover
    nats_host, nats_port = nats_image.run()
    print("Started natsd docker")
    yield f"nats://{nats_host}:{nats_port}"
    nats_image.stop()
