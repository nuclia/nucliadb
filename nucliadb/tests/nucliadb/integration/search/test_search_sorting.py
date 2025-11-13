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

from nucliadb_models.search import SearchOptions


@pytest.mark.deploy_modes("standalone")
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
@pytest.mark.deploy_modes("standalone")
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
            "features": ["fulltext", "keyword"],
            "show": ["origin"],
        },
    )
    assert resp.status_code == 200

    body = resp.json()
    for results in [body["fulltext"]["results"], body["paragraphs"]["results"]]:
        assert len(results) > 0

        sort_fields = [
            datetime.fromisoformat(body["resources"][result["rid"]]["origin"][sort_field])
            for result in results
        ]
        assert sort_fields == sort_function(sort_fields)


@pytest.mark.deploy_modes("standalone")
async def test_sorted_pagination(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    philosophy_books_kb,
):
    kbid = philosophy_books_kb

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "features": ["fulltext", "keyword"],
            "sort": {"field": "created", "order": "desc"},
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    fulltext_results = body["fulltext"]["results"]
    paragraphs_results = body["paragraphs"]["results"]

    #
    # FULLTEXT
    #
    # Page result by result, ensure we geºt the same results
    for index, result in enumerate(fulltext_results):
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/search",
            json={
                "query": "philosophy",
                "features": ["fulltext"],
                "sort": {"field": "created", "order": "desc"},
                "top_k": 1,
                "offset": index,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["fulltext"]["results"][0] == result

    # One more result, should not exist
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "features": ["fulltext"],
            "sort": {"field": "created", "order": "desc"},
            "top_k": 1,
            "offset": len(fulltext_results),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["fulltext"]["results"]) == 0

    #
    # PARAGRAPH
    #
    # Page result by result, ensure we get the same results
    for index, result in enumerate(paragraphs_results):
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/search",
            json={
                "query": "philosophy",
                "features": ["keyword"],
                "sort": {"field": "created", "order": "desc"},
                "top_k": 1,
                "offset": index,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["paragraphs"]["results"][0] == result

    # One more result, should not exist
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": "philosophy",
            "features": ["keyword"],
            "sort": {"field": "created", "order": "desc"},
            "top_k": 1,
            "offset": len(paragraphs_results),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["paragraphs"]["results"]) == 0


@pytest.mark.deploy_modes("standalone")
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
            datetime.fromisoformat(body["resources"][result["rid"]]["created"]) for result in results
        ]
        assert creation_dates == sorted(creation_dates, reverse=True)


@pytest.mark.deploy_modes("standalone")
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
                    "features": [SearchOptions.FULLTEXT.value],
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

            next_page = body["fulltext"]["next_page"]
            page_number += 1

        assert len(fulltext) == 10
        assert len(resources) == 10

        for results in [fulltext]:
            sort_fields = [
                datetime.fromisoformat(resources[result["rid"]][sort_field]) for result in results
            ]
            assert sort_fields == sort_function(sort_fields)  # type: ignore
