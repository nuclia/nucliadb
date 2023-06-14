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
from typing import List, Optional

import pytest
from fastapi.staticfiles import StaticFiles
from httpx import AsyncClient
from redis import asyncio as aioredis
from starlette.routing import Mount

from nucliadb.ingest.cache import clear_ingest_cache
from nucliadb.search import API_PREFIX
from nucliadb.search.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb.standalone.settings import Settings
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.utilities import clear_global_cache


@pytest.fixture(scope="function")
async def nucliadb_api(
    redis,
    # order matters here because of config diffs between redis/nats pubsub
    ingest_consumers,
    transaction_utility,
    indexing_utility_registered,
    test_settings_search: None,
    event_loop,
):  # type: ignore
    from nucliadb.common.cluster import manager
    from nucliadb.standalone.run import get_server

    app, _ = get_server(Settings())
    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    # Little hack to raise exeptions from VersionedFastApi
    async def handler(req, exc):  # type: ignore
        raise exc

    for route in app.routes:
        if isinstance(route, Mount) and not isinstance(route.app, StaticFiles):
            route.app.middleware_stack.handler = handler  # type: ignore

    await app.router.startup()

    await asyncio.sleep(1)
    while len(manager.get_index_nodes()) < 2:
        print("awaiting cluster nodes - one fixtures.py")
        await asyncio.sleep(1)

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

        client = AsyncClient(app=app, base_url=client_base_url)  # type: ignore
        client.headers["X-NUCLIADB-ROLES"] = ";".join([role.value for role in roles])
        client.headers["X-NUCLIADB-USER"] = user

        return client

    yield make_client_fixture

    await app.router.shutdown()
    await driver.flushall()
    await driver.close(close_connection_pool=True)

    clear_ingest_cache()
    clear_global_cache()
    for node in manager.get_index_nodes():
        node._reader = None  # type: ignore
        node._writer = None  # type: ignore
        node._sidecar = None  # type: ignore


@pytest.fixture(scope="function")
async def knowledgebox_one(nucliadb_api):
    kbslug = str(uuid.uuid4())
    async with nucliadb_api(roles=[NucliaDBRoles.MANAGER]) as client:
        data = {"slug": kbslug}
        resp = await client.post(f"/{KBS_PREFIX}", json=data)
        assert resp.status_code == 201
        kbid = resp.json()["uuid"]
    yield kbid
    async with nucliadb_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.delete(f"/{KB_PREFIX}/{kbid}")
        assert resp.status_code == 200
