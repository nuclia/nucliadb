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
    # Create 10 resources
    count = 10
    for i in range(count):
        resp = await nucliadb_writer.post(
            f"/kb/{standalone_knowledgebox}/resources",
            json={
                "slug": f"myresource-{i}",
                "title": f"My Title {i}",
                "summary": f"My summary {i}",
                "icon": "text/plain",
                "texts": {
                    "textfield1": {"body": f"Some text {i}", "format": "PLAIN"},
                    "textfield2": {"body": f"Some other text {i}", "format": "PLAIN"},
                },
            },
        )
        assert resp.status_code == 201

    counters1_resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/counters")
    shards1_resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/shards")
    counters1 = counters1_resp.json()
    shards1 = shards1_resp.json()

    assert len(shards1["shards"]) == 1

    # Change max shard paragraphs to be half: this should force the shard to be split in two
    with patch.object(settings, "max_shard_paragraphs", counters1["paragraphs"] / 2):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        shards2_resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/shards")
        shards2 = shards2_resp.json()
        assert len(shards2["shards"]) == 2


@pytest.mark.deploy_modes("standalone")
async def test_rebalance_merges_kb_shards(
    app_context,
    standalone_knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
    # Create 10 resources that will end up in 4 different shards
    count = 10
    for i in range(count):
        if i in [3, 6, 9]:
            await create_shard_for_kb(standalone_knowledgebox)

        # Each resource has 2 paragraphs (title and summary)
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
    assert counters["resources"] == 10
    assert counters["paragraphs"] == 20

    resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/shards")
    shards = resp.json()

    assert len(shards["shards"]) == 4

    # Change max shard paragraphs to be bigger: this should force all shards to be merged into one
    with patch.object(settings, "max_shard_paragraphs", 500):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        # Run again so that the cleanup of empty shards kicks-in
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

        resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/shards")
        shards = resp.json()

        assert len(shards["shards"]) == 1


async def create_shard_for_kb(kbid: str):
    async with datamanagers.with_rw_transaction() as txn:
        sm = get_shard_manager()
        await sm.create_shard_by_kbid(txn, kbid)
        await txn.commit()
