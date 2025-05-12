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
import os

import pytest
from httpx import AsyncClient
from nidx_protos.nodereader_pb2 import GetShardRequest, SearchRequest
from nidx_protos.noderesources_pb2 import ShardId

from nucliadb.common.datamanagers.cluster import KB_SHARDS
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.nidx import get_nidx_api_client, get_nidx_searcher_client
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.tests.vectors import Q
from nucliadb_protos.writer_pb2 import Shards as PBShards

RUNNING_IN_GH_ACTIONS = os.environ.get("CI", "").lower() == "true"


@pytest.mark.flaky(reruns=5)
async def test_multiple_fuzzy_search_resource_all(
    cluster_nucliadb_search: AsyncClient, multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    resp = await cluster_nucliadb_search.get(
        f'/{KB_PREFIX}/{kbid}/search?query=own+test+"This is great"&highlight=true&top_k=20',
    )

    assert resp.status_code == 200, resp.content
    assert len(resp.json()["paragraphs"]["results"]) == 20

    # Expected results:
    # - 'text' should not be highlighted as we are searching by 'test' in the query
    # - 'This is great' should be highlighted because it is an exact query search
    # - 'own' should not be highlighted because it is considered as a stop-word
    assert (
        resp.json()["paragraphs"]["results"][0]["text"]
        == "My own text Ramon. <mark>This is great</mark> to be here. "
    )


@pytest.mark.flaky(reruns=3)
async def test_search_resource_all(
    cluster_nucliadb_search: AsyncClient,
    test_search_resource: str,
) -> None:
    kbid = test_search_resource
    await asyncio.sleep(1)
    resp = await cluster_nucliadb_search.get(
        f"/{KB_PREFIX}/{kbid}/search?query=own+text&split=true&highlight=true&text_resource=true",
    )
    assert resp.status_code == 200
    assert resp.json()["fulltext"]["query"] == "own text"
    assert resp.json()["paragraphs"]["query"] == "own text"
    assert resp.json()["paragraphs"]["results"][0]["start_seconds"] == [0]
    assert resp.json()["paragraphs"]["results"][0]["end_seconds"] == [10]
    assert (
        resp.json()["paragraphs"]["results"][0]["text"]
        == "My own <mark>text</mark> Ramon. This is great to be here. "
    )
    assert len(resp.json()["resources"]) == 1
    assert len(resp.json()["sentences"]["results"]) == 1

    # get shards ids

    driver = get_driver()
    async with driver.transaction(read_only=True) as txn:
        key = KB_SHARDS.format(kbid=kbid)
        async for key in txn.keys(key):
            value = await txn.get(key)
            assert value is not None
            shards = PBShards()
            shards.ParseFromString(value)
            shard_id = shards.shards[0].nidx_shard_id
            shard = await get_nidx_api_client().GetShard(GetShardRequest(shard_id=ShardId(id=shard_id)))
            assert shard.shard_id == shard_id
            assert shard.fields == 3
            assert shard.paragraphs == 2
            assert shard.sentences == 3

            request = SearchRequest()
            request.vectorset = "my-semantic-model"
            request.shard = shard_id
            request.body = "Ramon"
            request.result_per_page = 20
            request.document = True
            request.paragraph = True
            request.vector.extend(Q)
            request.min_score_semantic = -1.0

            response = await get_nidx_searcher_client().Search(request)  # type: ignore
            paragraphs = response.paragraph
            documents = response.document
            vectors = response.vector

            assert paragraphs.total == 1
            assert documents.total == 1

            # 0-19 : My own text Ramon
            # 20-45 : This is great to be here
            # 48-65 : Where is my beer?

            # Q : Where is my wine?
            results = [(x.doc_id.id, x.score) for x in vectors.documents]
            results.sort(reverse=True, key=lambda x: x[1])
            assert results[0][0].endswith("48-65")
            assert results[1][0].endswith("0-19")
            assert results[2][0].endswith("20-45")


async def test_search_with_facets(
    cluster_nucliadb_search: AsyncClient, multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    url = f"/{KB_PREFIX}/{kbid}/search?query=own+text&faceted=/classification.labels"

    resp = await cluster_nucliadb_search.get(url)
    data = resp.json()
    assert data["fulltext"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    assert (
        data["paragraphs"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    )

    # also just test short hand filter
    url = f"/{KB_PREFIX}/{kbid}/search?query=own+text&faceted=/l"
    resp = await cluster_nucliadb_search.get(url)
    data = resp.json()
    assert data["fulltext"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    assert (
        data["paragraphs"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    )
