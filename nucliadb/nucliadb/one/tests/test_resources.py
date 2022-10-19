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

import asyncio
import uuid
from enum import Enum
from os.path import dirname
from typing import List, Optional

import aioredis
import pytest
from httpx import AsyncClient
from starlette.routing import Mount

from nucliadb.ingest.cache import clear_ingest_cache
from nucliadb.ingest.tests.fixtures import free_port
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.search import API_PREFIX
from nucliadb.search.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb_utils.utilities import clear_global_cache


@pytest.fixture(scope="function")
async def apiclient(
    redis,
    transaction_utility,
    indexing_utility_registered,
    test_settings,
    event_loop,
):  # type: ignore
    from nucliadb.ingest.orm import NODES
    from nucliadb.one.app import application

    async def handler(req, exc):  # type: ignore
        raise exc

    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    # Little hack to raise exeptions from VersionedFastApi
    for route in application.routes:
        if isinstance(route, Mount):
            route.app.middleware_stack.handler = handler  # type: ignore

    await application.router.startup()

    await asyncio.sleep(1)
    while len(NODES) < 2:
        print("awaiting cluster nodes - one fixtures.py")
        await asyncio.sleep(4)

    def make_client_fixture(
        roles: Optional[List[Enum]] = None,
        user: str = "",
        version: str = "1",
        root: bool = False,
    ) -> AsyncClient:
        roles = roles or []
        client_base_url = "http://test"

        if root is False:
            client_base_url = f"{client_base_url}/{API_PREFIX}/v{version}"

        client = AsyncClient(app=application, base_url=client_base_url)  # type: ignore
        client.headers["X-NUCLIADB-ROLES"] = ";".join([role.value for role in roles])
        client.headers["X-NUCLIADB-USER"] = user

        return client

    yield make_client_fixture

    try:
        await application.router.shutdown()
    except Exception:
        pass
    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()
    clear_ingest_cache()
    clear_global_cache()
    for node in NODES.values():
        node._reader = None
        node._writer = None
        node._sidecar = None


@pytest.fixture(scope="function")
def test_settings(gcs, natsd, tikv_driver, node):
    from nucliadb.ingest.settings import settings as ingest_settings
    from nucliadb_utils.cache.settings import settings as cache_settings
    from nucliadb_utils.settings import (
        nuclia_settings,
        nucliadb_settings,
        running_settings,
        storage_settings,
    )
    from nucliadb_utils.storages.settings import settings as extended_storage_settings

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = "gcs"
    storage_settings.gcs_bucket = "test_{kbid}"

    extended_storage_settings.gcs_indexing_bucket = "indexing"
    extended_storage_settings.gcs_deadletter_bucket = "deadletter"


    cache_settings.cache_pubsub_driver = "nats"
    cache_settings.cache_pubsub_channel = "pubsub-nuclia"
    cache_settings.cache_pubsub_nats_url = natsd

    running_settings.debug = False

    ingest_settings.pull_time = 0
    ingest_settings.driver = "tikv"
    ingest_settings.driver_tikv_url = tikv_driver.url
    ingest_settings.nuclia_partitions = 1

    nuclia_settings.dummy_processing = True
    ingest_settings.grpc_port = free_port()

    nucliadb_settings.nucliadb_ingest = f"localhost:{ingest_settings.grpc_port}"

    extended_storage_settings.local_testing_files = f"{dirname(__file__)}"


@pytest.fixture(scope="function")
async def kbid(apiclient):
    kbslug = str(uuid.uuid4())
    async with apiclient(roles=[NucliaDBRoles.MANAGER]) as client:
        data = {"slug": kbslug}
        resp = await client.post(f"/{KBS_PREFIX}", json=data)
        assert resp.status_code == 201
        kbid = resp.json()["uuid"]
    yield kbid
    async with apiclient(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.delete(f"/{KB_PREFIX}/{kbid}")
        assert resp.status_code == 200


async def create_resource(nucliadb_api, kb):
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        return await client.post(
            f"/{KB_PREFIX}/{kb}/resources",
            json={
                "title": "Resource title",
                "summary": "A simple summary",
            },
            headers={"X-SYNCHRONOUS": "TRUE"},
        )


async def get_resource(nucliadb_api, kb, rid):
    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        return await client.get(
            f"/{KB_PREFIX}/{kb}/resource/{rid}",
        )



@pytest.mark.asyncio
async def test_post_sync(
    apiclient, kbid
) -> None:
    async with apiclient(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/resources",
            json={
                "title": "Resource title",
                "summary": "A simple summary",
            },
            headers={"X-SYNCHRONOUS": "TRUE"},
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]
    
    resp = await get_resource(apiclient, kbid, rid)
    assert resp.status_code == 404


    # Try now waiting for a bit

    resp = await create_resource(apiclient, kbid)
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    import time
    time.sleep(2)

    resp = await get_resource(apiclient, kbid, rid)
    assert resp.status_code == 200

    pass


