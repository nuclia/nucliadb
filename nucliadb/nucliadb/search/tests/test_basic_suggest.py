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
from typing import Callable

import pytest
from httpx import AsyncClient
from nucliadb_protos.nodereader_pb2 import SuggestRequest
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.ingest.orm import NODES
from nucliadb.ingest.utils import get_driver
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.keys import KB_SHARDS


@pytest.mark.asyncio
async def test_search_kb_not_found(search_api: Callable[..., AsyncClient]) -> None:
    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/00000000000000/suggest?query=own+text",
        )
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_suggest_resource_all(
    search_api: Callable[..., AsyncClient], test_search_resource: str
) -> None:
    kbid = test_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/suggest?query=own+text",
        )
        assert resp.status_code == 200
        paragraph_results = resp.json()["paragraphs"]["results"]
        assert len(paragraph_results) == 1

    # get shards ids

    driver = await get_driver()
    txn = await driver.begin()
    key = KB_SHARDS.format(kbid=kbid)
    async for key in txn.keys(key):
        value = await txn.get(key)
        assert value is not None
        shards = PBShards()
        shards.ParseFromString(value)
        for replica in shards.shards[0].replicas:
            node_obj = NODES.get(replica.node)

            if node_obj is not None:
                shard = await node_obj.get_shard(replica.shard.id)
                assert shard.id == replica.shard.id
                shard_reader = await node_obj.get_reader_shard(replica.shard.id)
                assert shard_reader.resources == 3
                assert shard_reader.paragraphs == 2
                assert shard_reader.sentences == 3

                prequest = SuggestRequest()
                prequest.shard = replica.shard.id
                prequest.body = "Ramon"

                suggest = await node_obj.reader.Suggest(prequest)  # type: ignore
                assert suggest.total == 1
    await txn.abort()
