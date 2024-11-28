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

import asyncio
from unittest import mock
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.search.search.rank_fusion import LegacyRankFusion
from nucliadb_models.search import SearchOptions
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.exceptions import LimitsExceededError


@pytest.mark.asyncio
async def test_find_with_label_changes(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    await asyncio.sleep(1)

    # should get 1 result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    # assert we get no results with label filter
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "filters": ["/classification.labels/labels/label1"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0

    # add new label
    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{rid}",
        json={
            # "title": "My new title",
            "usermetadata": {
                "classifications": [
                    {
                        "labelset": "labels",
                        "label": "label1",
                        "cancelled_by_user": False,
                    }
                ],
                "relations": [],
            }
        },
    )
    assert resp.status_code == 200
    await asyncio.sleep(1)

    # we should get 1 result now with updated label
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "filters": ["/classification.labels/labels/label1"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1


@pytest.mark.asyncio
async def test_find_does_not_support_fulltext_search(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/find?query=title&features=fulltext&features=keyword",
    )
    assert resp.status_code == 422
    assert "fulltext search not supported" in resp.json()["detail"][0]["msg"]

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "features": [SearchOptions.FULLTEXT, SearchOptions.KEYWORD]},
    )
    assert resp.status_code == 422
    assert "fulltext search not supported" in resp.json()["detail"][0]["msg"]


@pytest.mark.asyncio
async def test_find_resource_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid1 = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid2 = resp.json()["uuid"]

    # Should get 2 result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 2

    # Check that resource filtering works
    for rid in [rid1, rid2]:
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/find",
            json={
                "query": "title",
                "resource_filters": [rid],
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) == 1
        assert rid in body["resources"]


async def test_find_min_score(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    # When not specifying the min score on the request
    # it should default to 0 for bm25 and 0.7 for semantic
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/find", json={"query": "dummy"})
    assert resp.status_code == 200
    assert resp.json()["min_score"] == {"bm25": 0, "semantic": 0.7}

    # When specifying the min score on the request
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "dummy", "min_score": {"bm25": 10, "semantic": 0.5}},
    )
    assert resp.status_code == 200
    assert resp.json()["min_score"] == {"bm25": 10, "semantic": 0.5}

    # Check that old api still works
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find", json={"query": "dummy", "min_score": 0.5}
    )
    assert resp.status_code == 200
    assert resp.json()["min_score"] == {"bm25": 0, "semantic": 0.5}


@pytest.mark.asyncio
async def test_story_7286(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{rid}",
        json={
            "fieldmetadata": [
                {
                    "field": {
                        "field": "text1",
                        "field_type": "text",
                    },
                    "paragraphs": [
                        {
                            "key": f"{rid}/t/text1/0-7",
                            "classifications": [{"labelset": "ls1", "label": "label"}],
                        }
                    ],
                }
            ]
        },
    )
    assert resp.status_code == 200

    with patch("nucliadb.search.search.hydrator.managed_serialize", return_value=None):
        # should get no result (because serialize returns None, as the resource is not found in the DB)
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/find",
            json={
                "query": "title",
                "features": [SearchOptions.KEYWORD, SearchOptions.SEMANTIC, SearchOptions.RELATIONS],
                "shards": [],
                "highlight": True,
                "autofilter": False,
                "page_number": 0,
                "show": ["basic", "values", "origin"],
                "filters": [],
            },
        )
        assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0


@pytest.mark.asyncio
async def test_find_marks_fuzzy_results(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
        },
    )
    assert resp.status_code == 201

    # Should get only one non-fuzzy result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=False, n_expected=1)

    # Should get only one fuzzy result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "totle",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=True, n_expected=1)

    # Should not get any result if exact match term queried
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": '"totle"',
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=True, n_expected=0)


def check_fuzzy_paragraphs(find_response, *, fuzzy_result: bool, n_expected: int):
    found = 0
    for resource in find_response["resources"].values():
        for field in resource["fields"].values():
            for paragraph in field["paragraphs"].values():
                assert paragraph["fuzzy_result"] is fuzzy_result
                found += 1
    assert found == n_expected


@pytest.mark.asyncio
async def test_find_returns_best_matches(
    nucliadb_reader: AsyncClient,
    philosophy_books_kb,
):
    kbid = philosophy_books_kb

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "and",
        },
    )
    assert resp.status_code == 200
    body = resp.json()

    best_matches = body["best_matches"]
    paragraphs = []
    for resource in body["resources"].values():
        for field in resource["fields"].values():
            for paragraph in field["paragraphs"].values():
                paragraphs.append(paragraph)
    assert len(paragraphs) == len(best_matches) > 2

    # Check that best matches is sorted by the paragraph order
    sorted_paragraphs = sorted(paragraphs, key=lambda p: p["order"])
    assert [p["id"] for p in sorted_paragraphs] == best_matches


@pytest.fixture(scope="function")
def find_with_limits_exceeded_error():
    with mock.patch(
        "nucliadb.search.api.v1.find.find",
        side_effect=LimitsExceededError(402, "over the quota"),
    ):
        yield


@pytest.mark.asyncio()
async def test_find_handles_limits_exceeded_error(
    nucliadb_reader, knowledgebox, find_with_limits_exceeded_error
):
    kb = knowledgebox
    resp = await nucliadb_reader.get(f"/kb/{kb}/find")
    assert resp.status_code == 402
    assert resp.json() == {"detail": "over the quota"}

    resp = await nucliadb_reader.post(f"/kb/{kb}/find", json={})
    assert resp.status_code == 402
    assert resp.json() == {"detail": "over the quota"}


async def test_find_keyword_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    kbid = knowledgebox
    # Create a couple of resources with different keywords in the title
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Friedrich Nietzsche. Beyond Good and Evil",
            "summary": "[SKU-123:4] The book is a treatise on the nature of morality and ethics. It was written by Friedrich Nietzsche.",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    nietzsche_rid = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Immanuel Kant. Critique of Pure Reason",
            "summary": "[SKU-567:8] The book is a treatise on metaphysics. It was written by Immanuel Kant.",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    kant_rid = resp.json()["uuid"]

    for keyword_filters, expected_rids in [
        (
            [],
            [nietzsche_rid, kant_rid],
        ),
        (
            ["Nietzsche"],
            [nietzsche_rid],
        ),
        (
            ["Kant"],
            [kant_rid],
        ),
        (
            ["niEtZscHe"],
            [nietzsche_rid],
        ),
        (
            ["Friedrich Nietzsche"],
            [nietzsche_rid],
        ),
        # More complex expressions
        (
            [
                {"all": ["Friedrich Nietzsche", "Immanuel Kant"]},
            ],
            [],
        ),
        (
            [
                {"any": ["Friedrich Nietzsche", "Immanuel Kant"]},
            ],
            [nietzsche_rid, kant_rid],
        ),
        # Searching with ids that contain punctuation characters should work
        (
            ["SKU-123:4"],
            [nietzsche_rid],
        ),
        (
            ["SKU-567:8"],
            [kant_rid],
        ),
        # Negative tests (no results expected)
        (["Focault"], []),  # Keyword not present
        (["Nietz"], []),  # Partial matches
        (["Nietzsche Friedrich"], []),  # Wrong order
        (["Nietzche"], []),  # Typo -- missing 's'
    ]:
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "query": "treatise",
                "keyword_filters": keyword_filters,
            },
        )
        assert resp.status_code == 200, f"Keyword filters: {keyword_filters}"
        body = resp.json()
        assert len(body["resources"]) == len(
            expected_rids
        ), f"Keyword filters: {keyword_filters}, expected rids: {expected_rids}"
        for rid in expected_rids:
            assert (
                rid in body["resources"]
            ), f"Keyword filters: {keyword_filters}, expected rids: {expected_rids}"


async def test_find_highlight(
    nucliadb_reader: AsyncClient,
    philosophy_books_kb: str,
):
    kbid = philosophy_books_kb

    with patch("nucliadb.search.search.find.get_rank_fusion", return_value=LegacyRankFusion(window=20)):
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "query": "Who was Marcus Aurelius?",
                "features": ["keyword", "semantic", "relations"],
                "highlight": True,
            },
        )
        assert resp.status_code == 200

    body = resp.json()
    assert len(body["resources"]) == 1
    match = body["resources"].popitem()[1]["fields"]["/a/summary"]["paragraphs"].popitem()[1]
    assert match["order"] == 0
    assert match["score_type"] == "BM25"
    assert "<mark>Marcus</mark> Aurelius" in match["text"]
