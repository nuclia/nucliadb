# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
from collections.abc import AsyncIterator, Iterator
from typing import Any, cast

import nats
import pytest
from pytest_docker_fixtures import images
from pytest_docker_fixtures.containers._base import BaseImage  # type: ignore[import-untyped]

from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.utilities import start_nats_manager, stop_nats_manager

images.settings = cast(dict[str, Any], images.settings)
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
