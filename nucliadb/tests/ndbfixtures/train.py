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

import aiohttp
import pytest
from grpc import aio

from nucliadb.standalone.settings import Settings
from nucliadb.train.utils import start_shard_manager, stop_shard_manager
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.tests import free_port
from nucliadb_utils.utilities import (
    clear_global_cache,
)

# train_rest_api = deploy_mode("standalone") nucliadb_train
# train_client = deploy_mode("component") nucliadb_train_grpc -> TrainStub


@pytest.fixture(scope="function")
async def component_nucliadb_train_grpc(
    train_client,
):
    yield train_client


# TODO: this should be at ndbfixtures.ingest or similar
@pytest.fixture(scope="function")
async def nucliadb_grpc(nucliadb: Settings):
    stub = WriterStub(aio.insecure_channel(f"localhost:{nucliadb.ingest_grpc_port}"))
    return stub


@pytest.fixture(scope="function")
async def standalone_nucliadb_train(
    train_rest_api,
):
    yield train_rest_api


@pytest.fixture(scope="function")
async def train_rest_api(nucliadb: Settings):  # type: ignore
    async with aiohttp.ClientSession(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://localhost:{nucliadb.http_port}",
    ) as client:
        yield client


# Utils


@pytest.fixture(scope="function")
def test_settings_train(cache, gcs, fake_node, maindb_driver):  # type: ignore
    from nucliadb.train.settings import settings
    from nucliadb_utils.settings import (
        FileBackendConfig,
        running_settings,
        storage_settings,
    )

    # TODO: PATCH!!!

    running_settings.debug = False

    old_file_backend = storage_settings.file_backend
    old_gcs_endpoint_url = storage_settings.gcs_endpoint_url
    old_gcs_bucket = storage_settings.gcs_bucket
    old_grpc_port = settings.grpc_port

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = FileBackendConfig.GCS
    storage_settings.gcs_bucket = "test_{kbid}"
    settings.grpc_port = free_port()
    yield
    storage_settings.file_backend = old_file_backend
    storage_settings.gcs_endpoint_url = old_gcs_endpoint_url
    storage_settings.gcs_bucket = old_gcs_bucket
    settings.grpc_port = old_grpc_port


@pytest.fixture(scope="function")
async def train_api(test_settings_train: None, local_files):  # type: ignore
    from nucliadb.train.utils import start_train_grpc, stop_train_grpc

    await start_shard_manager()
    await start_train_grpc("testing_train")
    yield
    await stop_train_grpc()
    await stop_shard_manager()


@pytest.fixture(scope="function")
async def train_client(train_api):  # type: ignore
    from nucliadb.train.settings import settings

    channel = aio.insecure_channel(f"localhost:{settings.grpc_port}")
    yield TrainStub(channel)
    clear_global_cache()
