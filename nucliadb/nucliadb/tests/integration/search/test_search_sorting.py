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
from datetime import datetime

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_search_sort_by_score(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    philosophy_books_kb,
):
    """
    Regular (no empty) search sorts by score
    """
    kbid = philosophy_books_kb

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={
            "query": "philosophy",
        },
    )
    assert resp.status_code == 200
    body = resp.json()

    for search_type in ["fulltext", "paragraphs"]:
        results = body[search_type]["results"]
        scores = [result["score"] for result in results]
        assert scores == sorted(scores, reverse=True)


@pytest.mark.parametrize(
    "sort_options",
    [
        ("created", "asc", sorted),
        ("created", "desc", lambda x: list(reversed(sorted(x)))),
        ("modified", "asc", sorted),
        ("modified", "desc", lambda x: list(reversed(sorted(x)))),
    ],
)
@pytest.mark.asyncio
async def test_search_sorted_by_creation_and_modification_dates(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    philosophy_books_kb,
    sort_options,
):
    """Regular (no empty) search sorted by creation or modification dates without
    further filtering returns results without sort limit

    """
    kbid = philosophy_books_kb

    sort_field, sort_order, sort_function = sort_options

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={
            "query": "philosophy",
            "sort_field": sort_field,
            "sort_order": sort_order,
        },
    )
    assert resp.status_code == 200

    body = resp.json()
    for results in [body["fulltext"]["results"], body["paragraphs"]["results"]]:
        assert len(results) > 0

        sort_fields = [
            datetime.fromisoformat(body["resources"][result["rid"]][sort_field])
            for result in results
        ]
        assert sort_fields == sort_function(sort_fields)


@pytest.mark.parametrize(
    "sort_options",
    [
        ("title", "asc", sorted),
        ("title", "desc", lambda x: list(reversed(sorted(x)))),
    ],
)
@pytest.mark.asyncio
async def test_limited_sorted_search_of_most_relevant_results(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    philosophy_books_kb,
    sort_options,
):
    """Sort by limited criteria. When sorting by, for example, title, results will
    be limited to `sort_limit`.

    """
    kbid = philosophy_books_kb

    sort_field, sort_order, sort_function = sort_options

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "sort": {
                "field": sort_field,
            },
        },
    )
    # must pass a sort limit
    assert resp.status_code == 422

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "sort": {"field": sort_field, "order": sort_order, "limit": 10},
        },
    )
    assert resp.status_code == 200

    body = resp.json()
    for results in [body["fulltext"]["results"], body["paragraphs"]["results"]]:
        assert len(results) >= 2

        sort_fields = [
            body["resources"][result["rid"]][sort_field] for result in results
        ]
        assert sort_fields == sort_function(sort_fields)


@pytest.mark.asyncio
async def test_empty_query_search_for_ordered_resources_by_creation_date_desc(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    ten_dummy_resources_kb,
):
    """An empty query without sort options returns all resources sorted by
    descendent creation date.

    """
    kbid = ten_dummy_resources_kb

    resp = await nucliadb_reader.get(f"/kb/{kbid}/search")
    assert resp.status_code == 200

    body = resp.json()
    for results in [body["fulltext"]["results"], body["paragraphs"]["results"]]:
        assert len(results) > 1

        creation_dates = [
            datetime.fromisoformat(body["resources"][result["rid"]]["created"])
            for result in results
        ]
        assert creation_dates == sorted(creation_dates, reverse=True)


@pytest.mark.asyncio
async def test_list_all_resources_by_creation_and_modification_dates_with_empty_queries(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    ten_dummy_resources_kb,
):
    """Using a sort by creation date without limit we can get a list of resources
    ordered by this.

    """
    kbid = ten_dummy_resources_kb

    sort_options = [
        ("created", "asc", sorted),
        ("created", "desc", lambda x: list(reversed(sorted(x)))),
        ("modified", "asc", sorted),
        ("modified", "desc", lambda x: list(reversed(sorted(x)))),
    ]

    for sort_field, sort_order, sort_function in sort_options:
        resources = {}
        fulltext = []

        page_number = 0
        page_size = 2
        next_page = True

        while next_page:
            resp = await nucliadb_reader.get(
                f"/kb/{kbid}/search",
                params={
                    "query": "",
                    "features": ["document"],
                    "fields": ["a/title"],
                    "page_number": page_number,
                    "page_size": page_size,
                    "sort_field": sort_field,
                    "sort_order": sort_order,
                },
            )
            assert resp.status_code == 200

            body = resp.json()

            resources.update(body["resources"])
            fulltext.extend(body["fulltext"]["results"])

            next_page = body["fulltext"]["next_page"] or body["paragraphs"]["next_page"]
            page_number += 1

        assert len(fulltext) == 10
        assert len(resources) == 10

        for results in [fulltext]:
            sort_fields = [
                datetime.fromisoformat(resources[result["rid"]][sort_field])
                for result in results
            ]
            assert sort_fields == sort_function(sort_fields)  # type: ignore


@pytest.mark.asyncio
async def test_search_sorting_most_relevant_results_with_pagination(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    philosophy_books_kb,
):
    """Test sorting N most relevant results using pagination. Check two
    pages contain sorted and no repeated elements. Validate asking for
    a page outside the limit established gives empty results.

    """
    kbid = philosophy_books_kb

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={"query": "philosophy", "sort_field": "title", "sort_limit": 100},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["fulltext"]["total"] == 4
    assert body["paragraphs"]["total"] == 4

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "sort": {
                "field": "title",
                "limit": 3,
            },
        },
    )
    assert resp.status_code == 200
    one_page_response = resp.json()

    resources = {}
    fulltext = []
    paragraphs = []

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "sort": {
                "field": "title",
                "limit": 3,
            },
            "page_size": 2,
            "page_number": 0,
        },
    )
    assert resp.status_code == 200

    body = resp.json()
    resources.update(body["resources"])
    fulltext.extend(body["fulltext"]["results"])
    paragraphs.extend(body["paragraphs"]["results"])

    assert len(fulltext) == len(paragraphs) == 2

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search",
        params={
            "query": "philosophy",
            "sort_field": "title",
            "sort_limit": 3,
            "page_size": 2,
            "page_number": 1,
        },
    )
    assert resp.status_code == 200

    body = resp.json()
    resources.update(body["resources"])
    fulltext.extend(body["fulltext"]["results"])
    paragraphs.extend(body["paragraphs"]["results"])

    assert len(fulltext) == len(paragraphs) == 3

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "sort": {
                "field": "title",
                "limit": 3,
            },
            "page_size": 2,
            "page_number": 2,
        },
    )
    assert resp.status_code == 200

    body = resp.json()
    assert len(body["fulltext"]["results"]) == 0
    assert len(body["paragraphs"]["results"]) == 0

    assert fulltext == one_page_response["fulltext"]["results"][:3]
    assert paragraphs == one_page_response["paragraphs"]["results"][:3]
