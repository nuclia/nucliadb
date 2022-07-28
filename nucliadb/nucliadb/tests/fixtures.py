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
from nucliadb import Settings, config_nucliadb, run_async_nucliadb
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_protos.train_pb2_grpc import TrainStub
from httpx import AsyncClient
from nucliadb_writer import API_PREFIX
import pytest
import tempfile
from grpc import aio


def free_port() -> int:
    import socket

    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


@pytest.fixture(scope="function")
async def nucliadb():
    with tempfile.TemporaryDirectory() as tmpdir:
        settings = Settings()
        settings.blob = f"{tmpdir}/blob"
        settings.maindb = f"{tmpdir}/main"
        settings.node = f"{tmpdir}/node"
        settings.http = free_port()
        settings.grpc = free_port()
        settings.train = free_port()
        settings.log = "INFO"
        config_nucliadb(settings)
        server = await run_async_nucliadb(settings)
        yield settings
        await server.shutdown()


@pytest.fixture(scope="function")
async def nucliadb_reader(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://localhost:{nucliadb.http}/{API_PREFIX}/v1",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def nucliadb_writer(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "WRITER"},
        base_url=f"http://localhost:{nucliadb.http}/{API_PREFIX}/v1",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def nucliadb_manager(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
        base_url=f"http://localhost:{nucliadb.http}/{API_PREFIX}/v1",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def knowledgebox(nucliadb_manager: AsyncClient):
    resp = await nucliadb_manager.post("/kbs", json={"slug": "knowledgebox"})
    assert resp.status_code == 201
    return resp.json().get("uuid")


@pytest.fixture(scope="function")
async def nucliadb_grpc(nucliadb: Settings):
    stub = WriterStub(aio.insecure_channel(f"localhost:{nucliadb.grpc}"))
    return stub


@pytest.fixture(scope="function")
async def nucliadb_train(nucliadb: Settings):
    stub = TrainStub(aio.insecure_channel(f"localhost:{nucliadb.train}"))
    return stub
