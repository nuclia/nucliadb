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
from enum import Enum
from typing import AsyncIterator, Callable, List, Optional

import pytest
from httpx import AsyncClient
from redis import asyncio as aioredis

from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb.writer import API_PREFIX
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb.writer.settings import settings
from nucliadb.writer.tus import clear_storage
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.settings import (
    FileBackendConfig,
    nuclia_settings,
    nucliadb_settings,
    storage_settings,
)


@pytest.fixture(scope="function")
async def writer_api(
    redis,
    grpc_servicer: IngestFixture,
    gcs_storage_writer,
    transaction_utility,
    processing_utility,
    tus_manager,
    event_loop,
) -> AsyncIterator[Callable[[List[Enum], str, str], AsyncClient]]:
    nucliadb_settings.nucliadb_ingest = grpc_servicer.host
    from nucliadb.writer.app import create_application

    application = create_application()

    def make_client_fixture(
        roles: Optional[List[Enum]] = None,
        user: str = "",
        version: str = "1",
    ) -> AsyncClient:
        roles = roles or []
        client_base_url = "http://test"
        client_base_url = f"{client_base_url}/{API_PREFIX}/v{version}"

        client = AsyncClient(app=application, base_url=client_base_url)  # type: ignore
        client.headers["X-NUCLIADB-ROLES"] = ";".join(
            map(lambda role: role.value, roles)
        )
        client.headers["X-NUCLIADB-USER"] = user

        return client

    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    await application.router.startup()
    yield make_client_fixture
    await application.router.shutdown()
    clear_storage()

    await driver.flushall()
    await driver.close(close_connection_pool=True)


@pytest.fixture(scope="function")
async def gcs_storage_writer(gcs):
    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = FileBackendConfig.GCS
    storage_settings.gcs_bucket = "test_{kbid}"


@pytest.fixture(scope="function")
async def knowledgebox_writer(writer_api):
    async with writer_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "kbid1",
                "title": "My Knowledge Box",
            },
        )
        assert resp.status_code == 201
    kbid = resp.json().get("uuid")
    assert kbid is not None
    yield kbid


@pytest.fixture(scope="function")
async def resource(redis, writer_api, knowledgebox_writer):
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_writer}/resources",
            headers={"X-Synchronous": "true"},
            json={
                "slug": "resource1",
                "title": "Resource 1",
            },
        )
        assert resp.status_code == 201
        uuid = resp.json()["uuid"]

    return uuid


@pytest.fixture(scope="function")
async def processing_utility():
    nuclia_settings.dummy_processing = True
    nuclia_settings.onprem = True
    nuclia_settings.nuclia_jwt_key = "foobarkey"


@pytest.fixture(scope="function")
async def tus_manager(redis):
    settings.dm_redis_host = redis[0]
    settings.dm_redis_port = redis[1]
