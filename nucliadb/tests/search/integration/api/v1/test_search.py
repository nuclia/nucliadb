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

from nucliadb.search.api.v1.router import KB_PREFIX

RUNNING_IN_GH_ACTIONS = os.environ.get("CI", "").lower() == "true"


@pytest.mark.deploy_modes("cluster")
async def test_multiple_fuzzy_search_resource_all(
    nucliadb_search: AsyncClient, multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    resp = await nucliadb_search.get(
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


@pytest.mark.deploy_modes("cluster")
async def test_search_resource_all(
    nucliadb_search: AsyncClient,
    test_search_resource: str,
) -> None:
    kbid = test_search_resource
    await asyncio.sleep(1)
    resp = await nucliadb_search.get(
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


@pytest.mark.deploy_modes("cluster")
async def test_search_with_facets(nucliadb_search: AsyncClient, multiple_search_resource: str) -> None:
    kbid = multiple_search_resource

    url = f"/{KB_PREFIX}/{kbid}/search?query=own+text&faceted=/classification.labels"

    resp = await nucliadb_search.get(url)
    data = resp.json()
    assert data["fulltext"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    assert (
        data["paragraphs"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    )

    # also just test short hand filter
    url = f"/{KB_PREFIX}/{kbid}/search?query=own+text&faceted=/l"
    resp = await nucliadb_search.get(url)
    data = resp.json()
    assert data["fulltext"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    assert (
        data["paragraphs"]["facets"]["/classification.labels"]["/classification.labels/labelset1"] == 25
    )
