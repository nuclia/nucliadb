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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.cluster import rebalance
from nucliadb.common.cluster.rebalance import (
    RebalanceShard,
    count_resources_in_shard,
    get_resources_from_shard,
    needs_merge,
)
from nucliadb.common.cluster.settings import settings
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.driver import Driver
from nucliadb_protos import writer_pb2


@pytest.fixture()
async def app_context(natsd, storage, nucliadb):
    ctx = ApplicationContext()
    await ctx.initialize()
    ctx._nats_manager = AsyncMock()
    ctx._nats_manager.js.consumer_info = AsyncMock(return_value=MagicMock(num_pending=1))
    yield ctx
    await ctx.finalize()


@pytest.mark.deploy_modes("standalone")
async def test_rebalance_splits_kb_shards(
    app_context,
    standalone_knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    total_resources = 10
    total_paragraphs = total_resources
    for i in range(total_resources):
        await create_resource_with_paragraph(nucliadb_writer, standalone_knowledgebox, i)

    # Create another shard so that the previous one gets the excess of shards moved to the new shard
    await create_shard_for_kb(standalone_knowledgebox)

    # There should be 2 shards at this point
    kb_shards = await get_kb_shards(standalone_knowledgebox)
    assert len(kb_shards.shards) == 2

    shards_to_resources = await build_shard_resources_index(
        app_context.kv_driver, standalone_knowledgebox
    )
    assert len(shards_to_resources) == 2
    assert set(shards_to_resources.values()) == {0, 10}

    # Active shard should not have any resource yet
    active_shard = next(
        s for idx, s in enumerate(kb_shards.shards) if not s.read_only and idx == kb_shards.actual
    )
    assert shards_to_resources[active_shard.shard] == 0

    # Change max shard paragraphs to be half: this should force the shard to be split in two
    with patch.object(settings, "max_shard_paragraphs", total_paragraphs * 0.75):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        # Make sure that the paragraphs are properly balanced across shards
        shards_to_resources = await build_shard_resources_index(
            app_context.kv_driver, standalone_knowledgebox
        )
        assert len(shards_to_resources) == 2
        assert set(shards_to_resources.values()) == {7, 3}

        # There should be 2 shards at this point too
        kb_shards = await get_kb_shards(standalone_knowledgebox)
        assert len(kb_shards.shards) == 2

        # Excess has been moved to the active shard
        active_shard = next(
            s for idx, s in enumerate(kb_shards.shards) if not s.read_only and idx == kb_shards.actual
        )
        assert shards_to_resources[active_shard.shard] == 3


@pytest.mark.deploy_modes("standalone")
async def test_rebalance_merges_kb_shards(
    app_context,
    standalone_knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    total_resources = 10
    total_paragraphs = total_resources
    for i in range(total_resources):
        # Create a shard every 3 resources
        if i in [3, 6, 9]:
            await create_shard_for_kb(standalone_knowledgebox)
        await create_resource_with_paragraph(nucliadb_writer, standalone_knowledgebox, i)

    # Make sure the counts are the expected
    resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/counters")
    counters = resp.json()
    assert counters["resources"] == total_resources
    assert counters["paragraphs"] == total_paragraphs

    shards_to_resources = await build_shard_resources_index(
        app_context.kv_driver, standalone_knowledgebox
    )
    assert len(shards_to_resources) == 4
    assert set(shards_to_resources.values()) == {3, 3, 3, 1}

    # Change max shard paragraphs to be bigger: this should force all shards to be merged into one (plus the current active shard)
    with patch.object(settings, "max_shard_paragraphs", total_paragraphs * 5):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        shards_to_resources = await build_shard_resources_index(
            app_context.kv_driver, standalone_knowledgebox
        )
        assert len(shards_to_resources) == 2
        assert set(shards_to_resources.values()) == {1, 9}


@pytest.mark.deploy_modes("standalone")
async def test_rebalance_splits_and_merges_kb_shards(
    app_context,
    standalone_knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    kbid = standalone_knowledgebox
    max_shard_paragraphs = 10

    # Create a shard that will need splitting (+4 paragraphs)
    with patch.object(settings, "max_shard_paragraphs", max_shard_paragraphs):
        for i in range(max_shard_paragraphs + 4):
            await create_resource_with_paragraph(nucliadb_writer, kbid, i=i)

        # Then create a couple of shard that could be merged into one
        await create_shard_for_kb(kbid)
        await create_resource_with_paragraph(nucliadb_writer, kbid, i=14)
        await create_shard_for_kb(kbid)
        await create_resource_with_paragraph(nucliadb_writer, kbid, i=15)

        # Then create another shard that is just empty (the active one)
        await create_shard_for_kb(kbid)

        kb_shards = await get_kb_shards(kbid)
        assert len(kb_shards.shards) == 4

        # Check that the paragraphs distribution is the expected
        shards_to_resources = await build_shard_resources_index(app_context.kv_driver, kbid)
        assert set(shards_to_resources.values()) == {14, 1, 1, 0}

        # Active shard should not be present in the index as it doesn't have any resource yet
        active_shard = next(
            s for idx, s in enumerate(kb_shards.shards) if not s.read_only and idx == kb_shards.actual
        )
        assert shards_to_resources[active_shard.shard] == 0

        # Run rebalance
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        kb_shards = await get_kb_shards(kbid)
        assert len(kb_shards.shards) == 3

        # Check that the paragraphs distribution is the expected
        shards_to_resources = await build_shard_resources_index(app_context.kv_driver, kbid)
        assert set(shards_to_resources.values()) == {10, 6, 0}


async def create_shard_for_kb(kbid: str):
    async with datamanagers.with_rw_transaction() as txn:
        sm = get_shard_manager()
        await sm.create_shard_by_kbid(txn, kbid, prewarm_enabled=False)
        await txn.commit()


async def get_kb_shards(kbid: str) -> writer_pb2.Shards:
    kb_shards = await datamanagers.atomic.cluster.get_kb_shards(kbid=kbid)
    assert kb_shards is not None
    return kb_shards


async def create_resource_with_paragraph(nucliadb_writer: AsyncClient, kbid: str, i: int) -> str:
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": f"My resource {i}",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    return resp.json()["uuid"]


@pytest.mark.deploy_modes("standalone")
async def test_build_shard_resources_index(
    app_context,
    standalone_knowledgebox,
    nucliadb_writer: AsyncClient,
):
    kbid = standalone_knowledgebox

    # Create some resources
    rids = []
    for i in range(0, 30):
        rids.append(await create_resource_with_paragraph(nucliadb_writer, kbid, i))

    # Build the shard resources index
    shards_to_resources = await build_shard_resources_index(app_context.kv_driver, kbid)

    # Check that the index is correct
    assert len(shards_to_resources) == 1
    assert set(shards_to_resources.values()) == {30}

    resources_batch = await get_resources_from_shard(
        app_context.kv_driver, kbid, next(iter(shards_to_resources.keys())), n=1000
    )
    assert set(resources_batch) == set(rids)


async def build_shard_resources_index(driver: Driver, kbid: str) -> dict[str, int]:
    shards = await get_kb_shards(kbid)
    result = {}
    for shard in shards.shards:
        count = await count_resources_in_shard(driver, kbid, shard.shard)
        result[shard.shard] = count
    return result


def test_needs_rebalance_and_finds_target():
    with patch.object(settings, "max_shard_paragraphs", 1_000_000):
        shards = [
            RebalanceShard(
                id=str(i),
                nidx_id=str(i),
                paragraphs=paragraphs,
                active=active,
            )
            for i, (paragraphs, active) in enumerate(
                [
                    (1_000_114, False),
                    (1_000_053, False),
                    (1_000_038, False),
                    (960_001, False),
                    (947_585, False),
                    (942_161, False),
                    (940_040, False),
                    (936_741, False),
                    (926_385, False),
                    (918_645, False),
                    (913_837, False),
                    # This one cannot be merged with all others because all of them are above %90 of max capacity
                    # and we don't currently merge onto the active shard.
                    (
                        469_845,
                        False,
                    ),
                    (174_397, True),
                ]
            )
        ]
        shard_to_merge = next((s for s in shards if needs_merge(s, shards)), None)
        assert shard_to_merge is None
