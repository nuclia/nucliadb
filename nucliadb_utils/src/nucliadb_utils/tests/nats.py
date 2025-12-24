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

import socket
from collections.abc import AsyncIterator, Iterator

import nats
import pytest
from pytest_docker_fixtures import images  # type: ignore
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore

from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.utilities import start_nats_manager, stop_nats_manager

images.settings["nats"] = {
    "image": "nats",
    "version": "2.10.21",
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
def natsd() -> Iterator[str]:
    nats_host, nats_port = nats_image.run()
    print("Started natsd docker")
    yield f"nats://{nats_host}:{nats_port}"
    nats_image.stop()


@pytest.fixture(scope="function")
async def nats_server(natsd: str) -> AsyncIterator[str]:
    yield natsd

    # cleanup nats
    nc = await nats.connect(servers=[natsd])
    await nc.drain()
    await nc.close()


@pytest.fixture(scope="function")
async def nats_manager(nats_server: str) -> AsyncIterator[NatsConnectionManager]:
    ncm = await start_nats_manager("nucliadb_tests", [nats_server], None)
    yield ncm
    await stop_nats_manager()
