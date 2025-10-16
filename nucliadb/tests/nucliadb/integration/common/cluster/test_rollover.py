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
import random
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient
from pytest_mock import MockerFixture

from nucliadb.common import datamanagers
from nucliadb.common.cluster import rollover
from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.nidx import NidxUtility
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.standalone.settings import Settings


@pytest.fixture(scope="function")
async def app_context(
    # we use nucliadb fixture here to make sure everything is prepared for
    # standalone, but we need maindb_driver or we'll initialize it an no one
    # else will be able to require the fixture and get the proper driver. Too
    # hacky...
    maindb_driver: Driver,
    standalone_nucliadb: Settings,
):
    ctx = ApplicationContext()
    await ctx.initialize()

    ctx._nats_manager = MagicMock()
    ctx._nats_manager.js.consumer_info = AsyncMock(return_value=MagicMock(num_pending=1))

    yield ctx

    await ctx.finalize()


@pytest.mark.deploy_modes("standalone")
async def test_rollover_kb_index(
    app_context: ApplicationContext,
    standalone_knowledgebox: str,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    await _test_rollover_kb_index(
        app_context, standalone_knowledgebox, nucliadb_writer, nucliadb_reader, nucliadb_reader_manager
    )


@pytest.mark.deploy_modes("standalone")
async def test_rollover_kb_index_with_vectorsets(
    app_context: ApplicationContext,
    knowledgebox_with_vectorsets: str,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    await _test_rollover_kb_index(
        app_context,
        knowledgebox_with_vectorsets,
        nucliadb_writer,
        nucliadb_reader,
        nucliadb_reader_manager,
    )


@pytest.mark.deploy_modes("standalone")
async def test_rollover_kb_index_with_prewarm(
    app_context: ApplicationContext,
    maindb_driver: Driver,
    nidx_utility: NidxUtility,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
    standalone_knowledgebox: str,
    mocker: MockerFixture,
):
    kbid = standalone_knowledgebox

    # TODO: remove this patch when we implemente configure shards in nidx binding
    with mocker.patch.object(nidx_utility.api_client, "ConfigureShards", new=AsyncMock()):
        # enable pre-warm for this KB
        await KnowledgeBox.update(maindb_driver, kbid, prewarm_enabled=True)

    # TODO: use a spy instead of a mock once it is implemented on nidx
    # spy = mocker.spy(nidx_utility.api_client, "ConfigureShards")
    with mocker.patch.object(nidx_utility.api_client, "ConfigureShards", new=AsyncMock()):
        await _test_rollover_kb_index(
            app_context, kbid, nucliadb_writer, nucliadb_reader, nucliadb_reader_manager
        )

        assert nidx_utility.api_client.ConfigureShards.call_count == 1
        for shard_config in nidx_utility.api_client.ConfigureShards.call_args.args[0].configs:
            assert shard_config.prewarm_enabled is True


async def _test_rollover_kb_index(
    app_context: ApplicationContext,
    kbid: str,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    count = 20
    for i in range(count):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "slug": f"myresource-{i}",
                "title": f"My Title {i}",
                "summary": f"My summary {i}",
                "icon": "text/plain",
            },
        )
        assert resp.status_code == 201

    resp = await nucliadb_reader_manager.get(f"/kb/{kbid}/shards")
    assert resp.status_code == 200, resp.text
    shards_body1 = resp.json()

    await rollover.rollover_kb_index(app_context, kbid)

    resp = await nucliadb_reader_manager.get(f"/kb/{kbid}/shards")
    assert resp.status_code == 200, resp.text
    shards_body2 = resp.json()
    # check that shards have changed
    assert shards_body1["shards"][0]["nidx_shard_id"] != shards_body2["shards"][0]["nidx_shard_id"]

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "title",
            "min_score": -100,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == count


@pytest.mark.deploy_modes("standalone")
async def test_rollover_kb_index_does_a_clean_cutover(
    app_context,
    knowledgebox,
):
    async def get_kb_shards(kbid: str):
        async with app_context.kv_driver.ro_transaction() as txn:
            return await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)

    shards1 = await get_kb_shards(knowledgebox)
    assert shards1.extra == {}

    await rollover.rollover_kb_index(app_context, knowledgebox)

    shards2 = await get_kb_shards(knowledgebox)
    assert shards2.extra == {}


@pytest.mark.deploy_modes("standalone")
async def test_rollover_kb_index_handles_changes_in_between(
    app_context,
    knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    count = 50
    resources = []
    for i in range(count):
        resp = await nucliadb_writer.post(
            f"/kb/{knowledgebox}/resources",
            json={
                "slug": f"myresource-{i}",
                "title": f"My Title {i}",
                "summary": f"My summary {i}",
                "icon": "text/plain",
            },
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]
        resources.append(rid)

    # Shuffle the list so that the deleted and modified resources are random
    shuffled_resources = resources.copy()
    random.shuffle(shuffled_resources)

    rollover_finished = asyncio.Event()

    async def the_rollover():
        try:
            await rollover.rollover_kb_index(app_context, knowledgebox)
        except asyncio.CancelledError:
            pass
        rollover_finished.set()

    # Start rollover in a separate asyncio task
    rollover_task = asyncio.create_task(the_rollover())
    try:
        # Delete a couple of resources while the rollover is running
        deleted_resources = []
        for i in range(3):
            rid_to_delete = shuffled_resources.pop(0)
            deleted_resources.append(rid_to_delete)
            resp = await nucliadb_writer.delete(f"/kb/{knowledgebox}/resource/{rid_to_delete}")
            assert resp.status_code == 204

        # Modify a couple of resources while the rollover is running
        modified_resources = []
        for i in range(3):
            rid_to_modify = shuffled_resources.pop(0)
            modified_resources.append(rid_to_modify)
            resp = await nucliadb_writer.patch(
                f"/kb/{knowledgebox}/resource/{rid_to_modify}",
                json={
                    "title": f"Modified Title {i}",
                },
            )
            assert resp.status_code == 200

        # Add a couple of resources while the rollover is running
        for i in range(2):
            resp = await nucliadb_writer.post(
                f"/kb/{knowledgebox}/resources",
                json={
                    "slug": f"my-added-resource-{i}",
                    "title": f"My Added Resource Title {i}",
                    "summary": f"My Added resource summary {i}",
                    "icon": "text/plain",
                },
            )
            assert resp.status_code == 201
            resources.append(resp.json()["uuid"])
            count += 1

    except Exception as ex:
        print("Exception caught: ", ex)
        rollover_task.cancel()
        raise
    else:
        assert not rollover_finished.is_set()
        # Wait for the rollover to finish
        await rollover_task

    # Check that the expected number of resources are in the new shards
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "title",
            "top_k": 100,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == count - len(deleted_resources)

    # Check that after the rollover has finished, the deleted resources are not found in the index
    for rid in deleted_resources:
        index = resources.index(rid)
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/find",
            json={
                "query": f'"My title {index}"',
                "fields": ["a/title"],
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) == 0

    # Modified resources should be updated in the new shards
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Modified",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == len(modified_resources)

    # Created resources should be in the new shards
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Added",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 2
