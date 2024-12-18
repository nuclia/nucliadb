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
from pytest_lazy_fixtures import lazy_fixture
from redis import asyncio as aioredis

from nucliadb.writer import tus
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb.writer.app import create_application
from nucliadb.writer.settings import settings
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.settings import (
    FileBackendConfig,
    nucliadb_settings,
    storage_settings,
)
from nucliadb_utils.tests.fixtures import get_testing_storage_backend
from nucliadb_utils.utilities import Utility, clean_utility, set_utility
from tests.ingest.fixtures import IngestFixture

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
    redis,
    storage_writer,
    grpc_servicer: IngestFixture,
    transaction_utility,
    dummy_processing,
    tus_manager,
    dummy_nidx_utility,
) -> AsyncIterator[FastAPI]:
    with patch.object(nucliadb_settings, "nucliadb_ingest", grpc_servicer.host):
        application = create_application()
        async with application.router.lifespan_context(application):
            yield application


# TODO: review
# Legacy from writer/fixtures.py


@pytest.fixture(scope="function")
def disabled_back_pressure():
    with patch("nucliadb.writer.back_pressure.is_back_pressure_enabled", return_value=False) as mocked:
        yield mocked


@pytest.fixture(scope="function")
def gcs_storage_writer(gcs):
    storage_settings.file_backend = FileBackendConfig.GCS
    storage_settings.gcs_endpoint_url = gcs
    storage_settings.gcs_bucket = "test_{kbid}"


@pytest.fixture(scope="function")
def s3_storage_writer(s3):
    storage_settings.file_backend = FileBackendConfig.S3
    storage_settings.s3_endpoint = s3
    storage_settings.s3_client_id = ""
    storage_settings.s3_client_secret = ""
    storage_settings.s3_bucket = "test-{kbid}"


def lazy_storage_writer_fixture():
    backend = get_testing_storage_backend()
    if backend == "gcs":
        return [lazy_fixture.lf("gcs_storage_writer")]
    elif backend == "s3":
        return [lazy_fixture.lf("s3_storage_writer")]
    else:
        print(f"Unknown storage backend {backend}, using gcs")
        return [lazy_fixture.lf("gcs_storage_writer")]


@pytest.fixture(scope="function", params=lazy_storage_writer_fixture())
async def storage_writer(request):
    """
    Generic storage fixture that allows us to run the same tests for different storage backends.
    """
    storage_driver = request.param
    set_utility(Utility.STORAGE, storage_driver)

    yield storage_driver

    clean_utility(Utility.STORAGE)


# TODO: this should be knowledgebox_onprem
@pytest.fixture(scope="function")
async def knowledgebox_writer(nucliadb_writer_manager: AsyncClient):
    resp = await nucliadb_writer_manager.post(
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
async def resource(redis, nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_writer}/resources",
        json={
            "slug": "resource1",
            "title": "Resource 1",
        },
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    return uuid


@pytest.fixture(scope="function")
async def tus_manager(redis):
    with (
        patch.object(settings, "dm_redis_host", redis[0]),
        patch.object(settings, "dm_redis_port", redis[1]),
    ):
        driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
        await driver.flushall()

        yield

        await tus.finalize()

        await driver.flushall()
        await driver.aclose(close_connection_pool=True)
