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
import datetime
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterable
from unittest.mock import AsyncMock, Mock, patch

import pytest
from httpx import AsyncClient
from pytest import LogCaptureFixture

from nucliadb.common.nidx import NidxUtility
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.deleter import DeleteBatch
from nucliadb_models.resource import ResourceList
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.nidx import SEARCHER_REFRESH_INTERVAL_SECONDS
from tests.ndbfixtures.resources.simples import create_simple_resource


@pytest.fixture(autouse=True)
def patch_back_pressure_materializer():
    processing_client = AsyncMock()
    processing_client.stats.return_value = Mock()
    processing_client.stats.return_value.incomplete = 10
    with patch("nucliadb.common.back_pressure.materializer.ProcessingHTTPClient", new=processing_client):
        yield


@pytest.fixture(autouse=True)
def patch_deleter_batch_size():
    # use a small batch size in order to trigger multiple batches and test that logic
    with patch("nucliadb.tasks.deleter.BATCH_SIZE", 2):
        yield


@pytest.fixture
async def simple_resources(
    nidx,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> AsyncIterable[list[str]]:
    kbid = knowledgebox
    rids = [
        await create_simple_resource(kbid, f"my simple {i}", nucliadb_writer, nucliadb_ingest_grpc)
        for i in range(5)
    ]
    await asyncio.sleep(SEARCHER_REFRESH_INTERVAL_SECONDS)
    yield rids


@pytest.mark.deploy_modes("component")
async def test_batch_delete_resources_by_rid(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    ingest_deleter_consumer: NatsTaskConsumer[DeleteBatch],
    nidx_utility: NidxUtility,
    knowledgebox: str,
    simple_resources: list[str],
    caplog: LogCaptureFixture,
) -> None:
    kbid = knowledgebox
    rids = simple_resources

    resp = await nucliadb_reader.get(
        f"{KB_PREFIX}/{kbid}/resources",
        params={"size": 50},
    )
    assert resp.status_code == 200
    resource_list = ResourceList.model_validate(resp.json())
    assert len(resource_list.resources) == 5
    assert set((resource.id for resource in resource_list.resources)) == set(rids)

    async with wait_for_deleter_job(caplog, timeout=2.0):
        resp = await nucliadb_writer.request(
            "DELETE",
            f"{KB_PREFIX}/{kbid}/resources",
            json={
                "filter_expression": {
                    "field": {
                        "or": [{"prop": "resource", "id": rid} for rid in rids[0:3]],
                    },
                }
            },
        )
        assert resp.status_code == 200
        assert set(resp.json()["resources"]) == set(rids[0:3])

    resp = await nucliadb_reader.get(
        f"{KB_PREFIX}/{kbid}/resources",
        params={"size": 50},
    )
    assert resp.status_code == 200
    resource_list = ResourceList.model_validate(resp.json())
    assert len(resource_list.resources) == 2
    assert set((resource.id for resource in resource_list.resources)) == set(rids[3:])


@pytest.mark.deploy_modes("component")
async def test_batch_delete_resources_by_created_date(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    ingest_deleter_consumer: NatsTaskConsumer[DeleteBatch],
    nidx_utility: NidxUtility,
    knowledgebox: str,
    simple_resources: list[str],
    caplog: LogCaptureFixture,
) -> None:
    kbid = knowledgebox
    rids = simple_resources

    async with wait_for_deleter_job(caplog, timeout=2.0):
        resp = await nucliadb_writer.request(
            "DELETE",
            f"{KB_PREFIX}/{kbid}/resources",
            json={
                "filter_expression": {
                    "field": {
                        "prop": "created",
                        "until": datetime.datetime.now().isoformat(),
                    },
                },
            },
        )
        assert resp.status_code == 200
        assert set(resp.json()["resources"]) == set(rids)

    resp = await nucliadb_reader.get(f"{KB_PREFIX}/{kbid}/resources", params={"size": 50})
    assert resp.status_code == 200
    resource_list = ResourceList.model_validate(resp.json())
    assert len(resource_list.resources) == 0


@pytest.mark.deploy_modes("component")
async def test_batch_delete_resources_by_origin_metadata(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    ingest_deleter_consumer: NatsTaskConsumer[DeleteBatch],
    nidx_utility: NidxUtility,
    knowledgebox: str,
    simple_resources: list[str],
    caplog: LogCaptureFixture,
) -> None:
    kbid = knowledgebox
    rids = simple_resources

    async with wait_for_deleter_job(caplog, timeout=2.0):
        resp = await nucliadb_writer.request(
            "DELETE",
            f"{KB_PREFIX}/{kbid}/resources",
            json={
                "filter_expression": {
                    "field": {
                        "or": [{"prop": "origin_metadata", "field": "name", "value": "my simple 0"}],
                    },
                }
            },
        )
        assert resp.status_code == 200
        assert resp.json()["resources"] == [rids[0]]

    resp = await nucliadb_reader.get(
        f"{KB_PREFIX}/{kbid}/resources",
        params={"size": 50},
    )
    assert resp.status_code == 200
    resource_list = ResourceList.model_validate(resp.json())
    assert len(resource_list.resources) == 4
    assert set((resource.id for resource in resource_list.resources)) == set(rids[1:])


@asynccontextmanager
async def wait_for_deleter_job(caplog: LogCaptureFixture, *, timeout: float = 2.0):
    with caplog.at_level(logging.INFO):
        yield

        finished = False
        while not finished and timeout > 0.0:
            for log in caplog.records:
                # NOTE this is coupled with the log message from the deleter
                if log.msg == "Batch deletion job completed":
                    finished = True
                    break
            else:
                caplog.clear()
                await asyncio.sleep(0.5)
                timeout -= 0.5

        assert finished, "deleter task didn't finish after some time"
