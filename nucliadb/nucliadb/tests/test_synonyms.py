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


@pytest.mark.asyncio
async def test_api(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox,
):
    kbid = knowledgebox
    synonyms_url = f"/kb/{kbid}/synonyms"

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
async def knowledgebox_with_synonyms(nucliadb_writer, knowledgebox):
    kbid = knowledgebox
    synonyms_url = f"/kb/{kbid}/synonyms"
    kb_synonyms = {
        "synonyms": {
            "planet": ["earth", "globe", "sphere", "world"],
        }
    }
    resp = await nucliadb_writer.put(synonyms_url, json=kb_synonyms)
    assert resp.status_code == 204
    yield kbid


@pytest.mark.asyncio
@pytest.mark.skip("Will be implemented in another PR")
async def test_search_with_synonims(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox_with_synonyms,
):
    kbid = knowledgebox_with_synonyms

    # Create a resource
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": "myresource",
            "title": "Life on Earth",
            "summary": "All the secrets of this planet",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.post(f"/kb/{kbid}/search?query=planet")
    assert resp.status_code == 200
    body = resp.json()

    # Fulltext and paragraph search should match on
    # summary (term) and title (synonym)
    assert len(body["paragraphs"]["results"]) == 2
    assert len(body["fulltext"]["results"]) == 2
    assert body["resources"][rid]
