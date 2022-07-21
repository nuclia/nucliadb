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
from nucliadb_protos.nodereader_pb2 import (
    DocumentSearchRequest,
    ParagraphSearchRequest,
    VectorSearchRequest,
)
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb_ingest.orm import NODES
from nucliadb_ingest.tests.vectors import Q
from nucliadb_ingest.utils import get_driver
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_search.api.models import SearchClientType
from nucliadb_search.api.v1.router import KB_PREFIX
from nucliadb_utils.keys import KB_SHARDS


@pytest.mark.asyncio
async def test_search_kb_not_found(search_api: Callable[..., AsyncClient]) -> None:
    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/00000000000000/search?query=own+text",
        )
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_search_resource_all(
    search_api: Callable[..., AsyncClient], test_search_resource: str
) -> None:
    kbid = test_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/search?query=own+text&split=true&highlight=true",
        )
        assert resp.status_code == 200
        assert resp.json()["fulltext"]["query"] == "own text"
        assert resp.json()["paragraphs"]["query"] == "own text"
        assert (
            resp.json()["fulltext"]["results"][0]["text"]
            == "My <mark>own</mark> <mark>text</mark> Ramon. This is grea…"
        )
        assert (
            resp.json()["paragraphs"]["results"][0]["text"]
            == "My <mark>own</mark> <mark>text</mark> Ramon. This is great to be here. "
        )
        assert len(resp.json()["resources"]) == 1
        assert len(resp.json()["sentences"]) == 2

    async with search_api(roles=[NucliaDBRoles.READER], root=True) as client:
        resp = await client.get(
            "/accounting",
        )
        assert len(resp.json()) == 1
        # as the search didn't use a specific client, it will be accounted as API
        assert (
            resp.json()[
                f"{test_search_resource}_-_search_client_{SearchClientType.API.value}"
            ]
            == 1
        )

        resp = await client.get(
            "/accounting",
        )
        assert len(resp.json()) == 0

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

                prequest = ParagraphSearchRequest()
                prequest.id = shard.id
                prequest.body = "Ramon"
                prequest.result_per_page = 10
                prequest.reload = True

                drequest = DocumentSearchRequest()
                drequest.id = shard.id
                drequest.body = "Ramon"
                drequest.result_per_page = 10
                drequest.reload = True

                vrequest = VectorSearchRequest()
                vrequest.id = shard.id
                vrequest.vector.extend(Q)
                vrequest.reload = True

                paragraphs = await node_obj.reader.ParagraphSearch(prequest)  # type: ignore
                documents = await node_obj.reader.DocumentSearch(drequest)  # type: ignore

                assert paragraphs.total == 1
                assert documents.total == 1

                vectors = await node_obj.reader.VectorSearch(vrequest)  # type: ignore

                # 0-19 : My own text Ramon
                # 20-45 : This is great to be here
                # 48-65 : Where is my beer?

                # Q : Where is my wine?
                results = [(x.doc_id.id, x.score) for x in vectors.documents]
                results.sort(reverse=True, key=lambda x: x[1])
                assert results[0][0].endswith("48-65")
                assert results[1][0].endswith("0-19")
                assert results[2][0].endswith("20-45")

    await txn.abort()
