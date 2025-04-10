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

from nucliadb_models.search import SearchOptions


@pytest.mark.deploy_modes("standalone")
async def test_custom_synonyms_api(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    synonyms_url = f"/kb/{kbid}/custom-synonyms"

    # Delete first
    resp = await nucliadb_writer.delete(synonyms_url, timeout=None)
    assert resp.status_code == 204

    kb_synonyms = {
        "synonyms": {
            "planet": ["earth", "globe", "sphere", "world"],
            "martial": ["extraterrestrial"],
        }
    }

    # Create some synonyms
    resp = await nucliadb_writer.put(synonyms_url, json=kb_synonyms, timeout=None)
    assert resp.status_code == 204

    # Now read them
    resp = await nucliadb_reader.get(synonyms_url, timeout=None)
    assert resp.status_code == 200
    body = resp.json()
    assert body == kb_synonyms

    # Update one
    kb_synonyms["synonyms"]["planet"].remove("sphere")
    resp = await nucliadb_writer.put(synonyms_url, json=kb_synonyms, timeout=None)
    assert resp.status_code == 204

    # Check it was updated
    resp = await nucliadb_reader.get(synonyms_url, timeout=None)
    assert resp.status_code == 200
    body = resp.json()
    assert body == kb_synonyms

    # Delete them now
    resp = await nucliadb_writer.delete(synonyms_url, timeout=None)
    assert resp.status_code == 204

    # Check it was deleted
    resp = await nucliadb_reader.get(synonyms_url, timeout=None)
    assert resp.status_code == 200
    body = resp.json()
    assert body == {"synonyms": {}}


@pytest.fixture(scope="function")
async def standalone_knowledgebox_with_synonyms(
    nucliadb_writer: AsyncClient, standalone_knowledgebox: str
):
    kbid = standalone_knowledgebox
    synonyms_url = f"/kb/{kbid}/custom-synonyms"
    kb_synonyms = {
        "synonyms": {
            "planet": ["earth", "globe", "sphere", "world"],
        }
    }
    resp = await nucliadb_writer.put(synonyms_url, json=kb_synonyms)
    assert resp.status_code == 204
    yield kbid


@pytest.mark.deploy_modes("standalone")
async def test_search_with_synonyms(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox_with_synonyms: str,
):
    kbid = standalone_knowledgebox_with_synonyms

    # Create a resource with:
    # - the term on the summary
    # - one of the synonyms in the title
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Earth",
            "summary": "Planet",
        },
    )
    assert resp.status_code == 201
    planet_rid = resp.json()["uuid"]

    # Create another resource with the remaining
    # synonyms present in title and summary fields
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Globe Sphere",
            "summary": "World",
        },
    )
    assert resp.status_code == 201
    sphere_rid = resp.json()["uuid"]

    # Create another resource that does not match
    # with the term or any of its synonyms
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Tomatoes",
            "summary": "The tomatoe collection",
        },
    )
    assert resp.status_code == 201
    tomatoe_rid = resp.json()["uuid"]

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=dict(
            features=["keyword"],
            query="planet",
            with_synonyms=True,
            highlight=True,
        ),
    )
    assert resp.status_code == 200
    body = resp.json()

    # Paragraph and fulltext search should match on summary (term)
    # and title (synonym) for the two resources
    assert len(get_pararagraphs(body)) == 4
    assert body["resources"][planet_rid]
    assert body["resources"][sphere_rid]
    assert tomatoe_rid not in body["resources"]

    # Check that searching without synonyms matches only query term
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=dict(features=["keyword"], query="planet"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(get_pararagraphs(body)) == 1
    assert body["resources"][planet_rid]
    assert sphere_rid not in body["resources"]
    assert tomatoe_rid not in body["resources"]

    # Check that searching with a term that has synonyms and
    # one that doesn't matches all of them
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=dict(
            features=["keyword"],
            query="planet tomatoe",
            with_synonyms=True,
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(get_pararagraphs(body)) == 5
    assert body["resources"][planet_rid]
    assert body["resources"][sphere_rid]
    assert body["resources"][tomatoe_rid]


def get_pararagraphs(body):
    paragraphs = []
    for resource in body.get("resources", {}).values():
        for field in resource.get("fields", {}).values():
            paragraphs.extend(field.get("paragraphs", []))
    return paragraphs


@pytest.mark.deploy_modes("standalone")
async def test_search_errors_if_vectors_or_relations_requested(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json=dict(
            features=[SearchOptions.KEYWORD, SearchOptions.SEMANTIC, SearchOptions.RELATIONS],
            query="planet",
            with_synonyms=True,
        ),
    )
    assert resp.status_code == 412
    assert resp.json()["detail"] == (
        "Invalid query. Error in synonyms: Search with custom synonyms "
        "is only supported on paragraph and document search"
    )
