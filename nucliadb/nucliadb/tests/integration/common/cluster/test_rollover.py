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
import pytest
from httpx import AsyncClient

from nucliadb.common.cluster import rollover
from nucliadb.common.context import ApplicationContext

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def app_context(natsd, gcs_storage, nucliadb):
    ctx = ApplicationContext()
    await ctx.initialize()
    yield ctx
    await ctx.finalize()


@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_rollover_shards(
    app_context,
    knowledgebox,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_manager: AsyncClient,
):
    count = 10
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

    resp = await nucliadb_manager.get(f"/kb/{knowledgebox}/shards")
    shards_body1 = resp.json()

    await rollover.rollover_shards(app_context, knowledgebox)

    resp = await nucliadb_manager.get(f"/kb/{knowledgebox}/shards")
    shards_body2 = resp.json()
    # check that shards have changed
    assert (
        shards_body1["shards"][0]["replicas"][0]["shard"]["id"]
        != shards_body2["shards"][0]["replicas"][0]["shard"]["id"]
    )

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == count
