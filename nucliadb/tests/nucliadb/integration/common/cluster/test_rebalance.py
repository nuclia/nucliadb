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

from nucliadb.common.cluster import rebalance
from nucliadb.common.cluster.settings import settings
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
async def test_rebalance_kb_shards(
    app_context,
    standalone_knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader_manager: AsyncClient,
):
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

    with patch.object(settings, "max_shard_paragraphs", counters1["paragraphs"] / 2):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)

    shards2_resp = await nucliadb_reader_manager.get(f"/kb/{standalone_knowledgebox}/shards")
    shards2 = shards2_resp.json()
    assert len(shards2["shards"]) == 2

    # if we run it again, we should get another shard
    with patch.object(settings, "max_shard_paragraphs", counters1["paragraphs"] / 2):
        await rebalance.rebalance_kb(app_context, standalone_knowledgebox)


# TODO: test rebalance uses KB pre-warm config
