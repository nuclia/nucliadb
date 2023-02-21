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

from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCES_PREFIX


@pytest.mark.asyncio
async def test_last_seqid_is_stored_in_resource(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-SYNCHRONOUS": "True"},
        json={
            "texts": {
                "textfield1": {"body": "Some text", "format": "PLAIN"},
            }
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]
    seqid = data["seqid"]

    # last_seqid should be returned on a resource get
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert seqid == resp.json()["last_seqid"]

    # last_seqid should be returned when listing resources
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resources")
    resource_list = resp.json()["resources"]
    assert len(resource_list) > 0
    for resource in resource_list:
        assert "last_seqid" in resource


@pytest.mark.asyncio
async def test_resource_crud(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-Synchronous": "true"},
        json={
            "slug": "mykb",
            "title": "My KB",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "My KB"

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}",
        headers={"X-Synchronous": "true"},
        json={
            "title": "My updated KB",
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "My updated KB"

    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}",
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 204

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_resources(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    """
    - Create 20 resources
    - Then check that scrolling the whole resource list returns
      the created resources without errors.
    """
    rids = set()
    for _ in range(20):
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
            headers={"X-Synchronous": "true"},
            json={
                "title": "My resource",
            },
        )
        assert resp.status_code == 201
        rids.add(resp.json()["uuid"])

    got_rids = set()
    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox}/resources?size=10&page=0"
    )
    assert resp.status_code == 200
    for r in resp.json()["resources"]:
        got_rids.add(r["id"])

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox}/resources?size=10&page=1"
    )
    assert resp.status_code == 200
    for r in resp.json()["resources"]:
        got_rids.add(r["id"])

    assert got_rids == rids


@pytest.mark.asyncio
async def test_get_resource_field(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    slug = "my-resource"
    field = "text-field"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-Synchronous": "true"},
        json={
            "slug": slug,
            "title": "My Resource",
            "texts": {field: {"body": "test1", "format": "PLAIN"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}/text/{field}")
    assert resp.status_code == 200
    body_by_slug = resp.json()

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/slug/{slug}/text/{field}")
    assert resp.status_code == 200
    body_by_rid = resp.json()

    assert body_by_slug == body_by_rid
