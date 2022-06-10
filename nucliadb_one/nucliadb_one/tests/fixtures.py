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
from typing import List

import aioredis
import pytest
from httpx import AsyncClient
from starlette.routing import Mount

from nucliadb_ingest.cache import clear_ingest_cache
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_search import API_PREFIX
from nucliadb_search.api.v1.router import KB_PREFIX
from nucliadb_utils.utilities import clear_global_cache


@pytest.fixture(scope="function")
def test_settings_one(gcs, redis, node):  # type: ignore
    from nucliadb_ingest.settings import settings as ingest_settings
    from nucliadb_search.settings import settings as search_settings
    from nucliadb_utils.cache.settings import settings as cache_settings
    from nucliadb_utils.settings import (
        nucliadb_settings,
        running_settings,
        storage_settings,
    )
    from nucliadb_utils.storages.settings import settings as extended_storage_settings

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = "gcs"
    storage_settings.gcs_bucket = "test_{kbid}"

    url = f"redis://{redis[0]}:{redis[1]}"
    cache_settings.cache_pubsub_driver = "redis"
    cache_settings.cache_pubsub_channel = "pubsub-nuclia"
    cache_settings.cache_pubsub_redis_url = url

    running_settings.debug = False

    ingest_settings.pull_time = 0
    ingest_settings.driver = "redis"
    ingest_settings.driver_redis_url = url
    ingest_settings.swim_binding_port = 4440
    search_settings.swim_enabled = False

    nucliadb_settings.nucliadb_ingest = f"localhost:{ingest_settings.grpc_port}"

    ingest_settings.chitchat_peers_addr = [
        f'{node["writer1"]["host"]}:{node["writer1"]["swim"]}',
        f'{node["writer2"]["host"]}:{node["writer2"]["swim"]}',
    ]

    extended_storage_settings.local_testing_files = f"{dirname(__file__)}"


@pytest.fixture(scope="function")
async def nucliadb_api(redis, transaction_utility, test_settings_one: None, event_loop):  # type: ignore
    from nucliadb_ingest.orm import NODES
    from nucliadb_one.app import application

    async def handler(req, exc):  # type: ignore
        raise exc

    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    # Little hack to raise exeptions from VersionedFastApi
    for route in application.routes:
        if isinstance(route, Mount):
            route.app.middleware_stack.handler = handler  # type: ignore

    await application.router.startup()

    while len(NODES) < 2:
        await asyncio.sleep(4)

    def make_client_fixture(
        roles: List[Enum] = [], user: str = "", version: str = "1", root: bool = False
    ) -> AsyncClient:
        client_base_url = "http://test"

        if root is False:
            client_base_url = f"{client_base_url}/{API_PREFIX}/v{version}"

        client = AsyncClient(app=application, base_url=client_base_url)  # type: ignore
        client.headers["X-NUCLIADB-ROLES"] = ";".join([role.value for role in roles])
        client.headers["X-NUCLIADB-USER"] = user

        return client

    yield make_client_fixture

    await application.router.shutdown()
    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()
    clear_ingest_cache()
    clear_global_cache()


@pytest.fixture(scope="function")
async def knowledgebox_one(nucliadb_api):
    kbslug = str(uuid.uuid4())
    async with nucliadb_api(roles=[NucliaDBRoles.MANAGER]) as client:
        data = {"slug": kbslug}
        resp = await client.post(f"/{KB_PREFIX}", json=data)
        assert resp.status_code == 201
        kbid = resp.json()["uuid"]
    yield kbid
    async with nucliadb_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.delete(f"/{KB_PREFIX}/{kbid}")
        assert resp.status_code == 200
