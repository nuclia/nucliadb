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

from nucliadb.common.maindb.driver import Driver
from nucliadb.reader.app import create_application
from nucliadb.standalone.settings import Settings
from nucliadb.writer import API_PREFIX
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import (
    running_settings,
    transaction_settings,
)
from nucliadb_utils.storages.storage import Storage
from tests.utils.dirty_index import wait_for_sync

from .utils import create_api_client_factory

# Main fixtures


@pytest.fixture(scope="function")
async def component_nucliadb_reader(reader_api_server: FastAPI) -> AsyncIterator[AsyncClient]:
    with patch.object(running_settings, "debug", False):
        client_factory = create_api_client_factory(reader_api_server)
        async with client_factory(roles=[NucliaDBRoles.READER]) as client:
            yield client


@pytest.fixture(scope="function")
async def standalone_nucliadb_reader(standalone_nucliadb: Settings) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://localhost:{standalone_nucliadb.http_port}/{API_PREFIX}/v1",
        timeout=None,
        event_hooks={"request": [wait_for_sync]},
    ) as client:
        yield client


# Derived


@pytest.fixture(scope="function")
async def nucliadb_reader_manager(
    nucliadb_reader: AsyncClient,
) -> AsyncIterator[AsyncClient]:
    roles = [NucliaDBRoles.MANAGER, NucliaDBRoles.READER]
    nucliadb_reader.headers["X-NUCLIADB-ROLES"] = ";".join([role.value for role in roles])
    yield nucliadb_reader


# Helpers


@pytest.fixture(scope="function")
async def reader_api_server(
    storage: Storage,
    maindb_driver: Driver,
    nats_manager: NatsConnectionManager,
    dummy_nidx_utility,
) -> AsyncIterator[FastAPI]:
    application = create_application()
    with patch.object(transaction_settings, "transaction_local", True):
        async with application.router.lifespan_context(application):
            yield application
