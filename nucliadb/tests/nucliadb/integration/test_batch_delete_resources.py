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
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from pytest import LogCaptureFixture

from nucliadb.common.back_pressure.materializer import BackPressureMaterializer
from nucliadb.common.nidx import NidxUtility
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.deleter import DeleteBatch
from nucliadb_models.resource import ResourceList


@pytest.fixture(autouse=True)
def patch_deleter_batch_size():
    # use a small batch size in order to trigger multiple batches and test that logic
    with patch("nucliadb.tasks.deleter.BATCH_SIZE", 2):
        yield


@pytest.mark.deploy_modes("component")
async def test_batch_delete_resources_by_rid(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nidx_utility: NidxUtility,
    ingest_deleter_consumer: NatsTaskConsumer[DeleteBatch],
    back_pressure_materializer: BackPressureMaterializer,
    simple_resources: tuple[str, list[str]],
    caplog: LogCaptureFixture,
) -> None:
    kbid, rids = simple_resources

    resp = await nucliadb_reader.get(
        f"{KB_PREFIX}/{kbid}/resources",
        params={"size": 50},
    )
    assert resp.status_code == 200
    resource_list = ResourceList.model_validate(resp.json())
    assert len(resource_list.resources) == 10
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
    assert len(resource_list.resources) == 7
    assert set((resource.id for resource in resource_list.resources)) == set(rids[3:])


@pytest.mark.deploy_modes("component")
async def test_batch_delete_resources_by_created_date(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nidx_utility: NidxUtility,
    ingest_deleter_consumer: NatsTaskConsumer[DeleteBatch],
    back_pressure_materializer: BackPressureMaterializer,
    simple_resources: tuple[str, list[str]],
    caplog: LogCaptureFixture,
) -> None:
    kbid, rids = simple_resources

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
    nidx_utility: NidxUtility,
    ingest_deleter_consumer: NatsTaskConsumer[DeleteBatch],
    back_pressure_materializer: BackPressureMaterializer,
    simple_resources: tuple[str, list[str]],
    caplog: LogCaptureFixture,
) -> None:
    kbid, rids = simple_resources

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
    assert len(resource_list.resources) == 9
    assert set((resource.id for resource in resource_list.resources)) == set(rids[1:])


@pytest.mark.deploy_modes("component")
async def test_batch_delete_resources_with_back_pressure(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nidx_utility: NidxUtility,
    ingest_deleter_consumer: NatsTaskConsumer[DeleteBatch],
    back_pressure_materializer: BackPressureMaterializer,
    knowledgebox: str,
    simple_resources: tuple[str, list[str]],
    caplog: LogCaptureFixture,
) -> None:
    kbid, rids = simple_resources

    def try_after(*args, **kwargs):
        return datetime.datetime.now()

    with (
        patch.object(back_pressure_materializer, "get_ingest_pending", side_effect=[100, 30, 5]),
        patch(
            "nucliadb.common.back_pressure.materializer.estimate_try_after",
            side_effect=try_after,
        ),
        caplog.at_level(logging.INFO),
    ):
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

        back_pressured = False
        for log in caplog.records:
            # NOTE this is coupled with the log message from the deleter
            if log.msg == "Deleter got back pressure":
                back_pressured = True
                break
        assert back_pressured, "deleter should have got back pressure"

        # after waiting the back pressure, the deletion still happens
        resp = await nucliadb_reader.get(
            f"{KB_PREFIX}/{kbid}/resources",
            params={"size": 50},
        )
        assert resp.status_code == 200
        resource_list = ResourceList.model_validate(resp.json())
        assert len(resource_list.resources) == 9
        assert set((resource.id for resource in resource_list.resources)) == set(rids[1:])


@asynccontextmanager
async def wait_for_deleter_job(caplog: LogCaptureFixture, *, timeout: float = 2.0):
    with caplog.at_level(logging.INFO):
        yield

        finished = False
        last_read = 0
        while not finished and timeout > 0.0:
            for log in caplog.records[last_read:]:
                # NOTE this is coupled with the log message from the deleter
                if log.msg == "Batch deletion job completed":
                    finished = True
                    break
            else:
                last_read = len(caplog.records)
                await asyncio.sleep(0.5)
                timeout -= 0.5

        assert finished, "deleter task didn't finish after some time"
