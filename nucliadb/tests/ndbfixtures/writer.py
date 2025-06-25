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
from typing import AsyncIterator
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from redis import asyncio as aioredis

from nucliadb.standalone.settings import Settings
from nucliadb.writer import API_PREFIX, tus
from nucliadb.writer.app import create_application
from nucliadb.writer.settings import settings
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.settings import (
    nucliadb_settings,
)
from nucliadb_utils.storages.storage import Storage
from tests.utils.dirty_index import mark_dirty

from .utils import create_api_client_factory

# TODO: replace knowledgebox_ingest for knowledgebox

# Main fixtures


@pytest.fixture(scope="function")
async def component_nucliadb_writer(
    writer_api_server: FastAPI,
) -> AsyncIterator[AsyncClient]:
    client_factory = create_api_client_factory(writer_api_server)
    async with client_factory(roles=[NucliaDBRoles.WRITER]) as client:
        yield client


@pytest.fixture(scope="function")
async def standalone_nucliadb_writer(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "WRITER"},
        base_url=f"http://localhost:{nucliadb.http_port}/{API_PREFIX}/v1",
        timeout=None,
        event_hooks={"request": [mark_dirty]},
    ) as client:
        yield client


# Derived


@pytest.fixture(scope="function")
async def nucliadb_writer_manager(
    nucliadb_writer: AsyncClient,
) -> AsyncIterator[AsyncClient]:
    roles = [NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER]
    nucliadb_writer.headers["X-NUCLIADB-ROLES"] = ";".join([role.value for role in roles])
    yield nucliadb_writer


# Helpers


@pytest.fixture(scope="function")
async def writer_api_server(
    disabled_back_pressure,
    valkey,
    storage: Storage,
    ingest_grpc_server,  # component fixture, shouldn't be used in other modes
    ingest_consumers,
    ingest_processed_consumer,
    learning_config,
    transaction_utility,
    dummy_processing,
    tus_manager,
    dummy_nidx_utility,
) -> AsyncIterator[FastAPI]:
    with patch.object(nucliadb_settings, "nucliadb_ingest", ingest_grpc_server.address):
        application = create_application()
        async with application.router.lifespan_context(application):
            yield application


# TODO: review
# Legacy from writer/fixtures.py


@pytest.fixture(scope="function")
def disabled_back_pressure():
    with patch(
        "nucliadb.common.back_pressure.materializer.is_back_pressure_enabled", return_value=False
    ) as mocked:
        yield mocked


@pytest.fixture(scope="function")
async def tus_manager(valkey):
    with (
        patch.object(settings, "dm_redis_host", valkey[0]),
        patch.object(settings, "dm_redis_port", valkey[1]),
    ):
        driver = aioredis.from_url(f"redis://{valkey[0]}:{valkey[1]}")
        await driver.flushall()

        yield

        await tus.finalize()

        await driver.flushall()
        await driver.aclose(close_connection_pool=True)
