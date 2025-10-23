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
from nucliadb.common.cluster.settings import settings
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.resources import KB_RESOURCE_SHARD


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
    total_paragraphs = 2 * total_resources
    for i in range(total_resources):
        resp = await nucliadb_writer.post(
            f"/kb/{standalone_knowledgebox}/resources",
            json={
                "slug": f"myresource-{i}",
                "title": f"My Title {i}",
                "summary": f"My summary {i}",
                "icon": "text/plain",
            },
        )
        assert resp.status_code == 201

    # Create another shard so that the previous one gets the excess of shards moved to the new shard
    await create_shard_for_kb(standalone_knowledgebox)

    shards_to_resources = await build_shard_resources_index(standalone_knowledgebox)
    assert len(shards_to_resources) == 1
    assert {len(resources) for resources in shards_to_resources.values()} == {10}

    # Change max shard paragraphs to be half: this should force the shard to be split in two
    with patch.object(settings, "max_shard_paragraphs", total_paragraphs * 0.75):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        # Make sure that the paragraphs are properly balanced across shards
        shards_to_resources = await build_shard_resources_index(standalone_knowledgebox)
        assert len(shards_to_resources) == 2
        assert {len(resources) for resources in shards_to_resources.values()} == {8, 2}


@pytest.mark.deploy_modes("standalone")
async def test_rebalance_merges_kb_shards(
    app_context,
    standalone_knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    total_resources = 10
    # Each resource has 2 paragraphs (title and summary)
    total_paragraphs = total_resources * 2
    for i in range(total_resources):
        if i in [3, 6, 9]:
            await create_shard_for_kb(standalone_knowledgebox)
        resp = await nucliadb_writer.post(
            f"/kb/{standalone_knowledgebox}/resources",
            json={
                "title": f"My resource {i}",
                "summary": f"My summary {i}",
                "icon": "text/plain",
            },
        )
        assert resp.status_code == 201

    # Make sure the counts are the expected
    resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/counters")
    counters = resp.json()
    assert counters["resources"] == total_resources
    assert counters["paragraphs"] == total_paragraphs

    shards_to_resources = await build_shard_resources_index(standalone_knowledgebox)
    assert len(shards_to_resources) == 4
    assert {len(resources) for resources in shards_to_resources.values()} == {3, 1, 3, 3}

    # Change max shard paragraphs to be bigger: this should force all shards to be merged into one (plus the current active shard)
    with patch.object(settings, "max_shard_paragraphs", total_paragraphs * 5):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        shards_to_resources = await build_shard_resources_index(standalone_knowledgebox)
        assert len(shards_to_resources) == 2
        assert {len(resources) for resources in shards_to_resources.values()} == {1, 9}


async def create_shard_for_kb(kbid: str):
    async with datamanagers.with_rw_transaction() as txn:
        sm = get_shard_manager()
        await sm.create_shard_by_kbid(txn, kbid)
        await txn.commit()


async def build_shard_resources_index(kbid: str) -> dict[str, set[str]]:
    index: dict[str, set[str]] = {}
    rids = [rid async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid)]
    async with datamanagers.with_ro_transaction() as txn:
        shards = await txn.batch_get(
            keys=[KB_RESOURCE_SHARD.format(kbid=kbid, uuid=rid) for rid in rids],
            for_update=False,
        )
        for rid, shard_bytes in zip(rids, shards):
            if shard_bytes is not None:
                index.setdefault(shard_bytes.decode(), set()).add(rid)
    return index
