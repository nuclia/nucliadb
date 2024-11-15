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
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.settings import running_settings
from nucliadb_utils.storages.storage import Storage

from .utils import create_api_client_factory

# Main fixtures


@pytest.fixture(scope="function")
async def component_nucliadb_reader(
    # XXX: there's some weird dependency with `local_files` fixtures. Without
    # it, download tests fail only if they are run after a test without using
    # this fixture. This should be fixed or explained
    local_files,
    dummy_index_node_cluster,
    dummy_nidx_utility,
    reader_api_server: FastAPI,
) -> AsyncIterator[AsyncClient]:
    with patch.object(running_settings, "debug", False):
        client_factory = create_api_client_factory(reader_api_server)
        async with client_factory(roles=[NucliaDBRoles.READER]) as client:
            yield client


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
) -> AsyncIterator[FastAPI]:
    application = create_application()
    async with application.router.lifespan_context(application):
        yield application
