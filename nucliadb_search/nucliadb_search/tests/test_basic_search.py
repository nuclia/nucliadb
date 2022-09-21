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
import math
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
from nucliadb_search.api.models import NucliaDBClientType
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
async def test_search_sc_2104(
    search_api: Callable[..., AsyncClient], test_search_resource: str
) -> None:
    kbid = test_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{kbid}/search?queri=own+text")
        assert resp.status_code == 422


@pytest.mark.asyncio
async def test_multiple_fuzzy_search_resource_all(
    search_api: Callable[..., AsyncClient], multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f'/{KB_PREFIX}/{kbid}/search?query=own+test+"This is great"&highlight=true&page_number=0&page_size=20',
        )
        assert len(resp.json()["paragraphs"]["results"]) == 20
        assert (
            resp.json()["paragraphs"]["results"][0]["text"]
            == "My <mark>own</mark> <mark>text</mark> Ramon. <mark>This is great</mark> to be here. "
        )


@pytest.mark.asyncio
async def test_multiple_search_resource_all(
    search_api: Callable[..., AsyncClient], multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        await asyncio.sleep(5)
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/search?query=own+text&highlight=true&page_number=0&page_size=40",
        )
        if resp.status_code != 200:
            print(resp.content)

        assert resp.status_code == 200
        assert len(resp.json()["paragraphs"]["results"]) == 40
        assert resp.json()["paragraphs"]["next_page"]
        assert resp.json()["fulltext"]["next_page"]

        shards = resp.json()["shards"]
        pos_30 = resp.json()["paragraphs"]["results"][30]
        pos_35 = resp.json()["paragraphs"]["results"][35]

        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/search?query=own+text&highlight=true&page_number=3&page_size=10&shards={shards[0]}",  # noqa
        )
        if resp.status_code != 200:
            print(resp.content)

        assert resp.status_code == 200
        assert resp.json()["shards"][0] == shards[0]
        assert resp.json()["paragraphs"]["results"][0]["rid"] == pos_30["rid"]
        assert resp.json()["paragraphs"]["results"][5]["rid"] == pos_35["rid"]

        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/search?query=own+text&highlight=true&page_number=4&page_size=20",
        )
        for _ in range(10):
            if (
                len(resp.json()["paragraphs"]["results"]) < 20
                or len(resp.json()["fulltext"]["results"]) < 20
            ):
                await asyncio.sleep(1)
                resp = await client.get(
                    f"/{KB_PREFIX}/{kbid}/search?query=own+text&highlight=true&page_number=4&page_size=20",
                )
            else:
                break
        assert len(resp.json()["paragraphs"]["results"]) == 20
        assert resp.json()["paragraphs"]["total"] == 20
        assert len(resp.json()["fulltext"]["results"]) == 20
        assert resp.json()["fulltext"]["total"] == 20

        assert resp.json()["paragraphs"]["next_page"] is False
        assert resp.json()["fulltext"]["next_page"] is False


@pytest.mark.asyncio
async def test_search_resource_all(
    search_api: Callable[..., AsyncClient], test_search_resource: str
) -> None:
    kbid = test_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        await asyncio.sleep(1)
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/search?query=own+text&split=true&highlight=true&text_resource=true",
        )
        assert resp.status_code == 200
        assert resp.json()["fulltext"]["query"] == "own text"
        assert resp.json()["paragraphs"]["query"] == "own text"
        assert resp.json()["paragraphs"]["results"][0]["start_seconds"] == [0]
        assert resp.json()["paragraphs"]["results"][0]["end_seconds"] == [10]
        assert (
            resp.json()["paragraphs"]["results"][0]["text"]
            == "My <mark>own</mark> <mark>text</mark> Ramon. This is great to be here. "
        )
        assert len(resp.json()["resources"]) == 1
        assert len(resp.json()["sentences"]["results"]) == 1

    async with search_api(roles=[NucliaDBRoles.READER], root=True) as client:
        resp = await client.get(
            "/accounting",
        )
        assert len(resp.json()) == 1
        # as the search didn't use a specific client, it will be accounted as API
        assert (
            resp.json()[
                f"{test_search_resource}_-_search_client_{NucliaDBClientType.API.value}"
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
                vrequest.result_per_page = 20
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


@pytest.mark.asyncio
async def test_search_pagination(
    search_api: Callable[..., AsyncClient], multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        await asyncio.sleep(2)

        n_results_expected = 100
        page_size = 20
        expected_requests = math.ceil(n_results_expected / page_size)

        results = []
        shards = None

        for request_n in range(expected_requests):

            url = f"/{KB_PREFIX}/{kbid}/search?query=own+text&highlight=true&page_number={request_n}&page_size={page_size}"  # noqa

            if shards is not None and len(shards):
                url += f"&shards={','.join(shards)}"

            resp = await client.get(url)

            if resp.status_code != 200:
                print(resp.content)

            assert resp.status_code == 200

            response = resp.json()

            for result in response["paragraphs"]["results"]:
                results.append(result["rid"])

            shards = response["shards"]

            if not response["paragraphs"]["next_page"]:
                # Pagination ended! No more results
                assert request_n == expected_requests - 1

        # Check that we iterated over all matching resources
        unique_results = set(results)
        assert len(unique_results) == n_results_expected
