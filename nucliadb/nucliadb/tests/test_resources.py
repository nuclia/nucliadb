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

from nucliadb_search.api.v1.router import KB_PREFIX


@pytest.mark.asyncio
async def test_last_seqid_in_resource(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/resources",
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

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert seqid == resp.json()["last_seqid"]

    # A resource update involving processing changes its seqid

    payloads = [
        {
            "texts": {
                "textfield2": {"body": "Another text", "format": "PLAIN"},
            }
        }
    ]

    for payload in payloads:
        resp = await nucliadb_writer.patch(
            f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}",
            headers={"X-SYNCHRONOUS": "True"},
            json=payload,
        )
        assert resp.status_code == 200
        new_seqid = resp.json()["seqid"]
        assert new_seqid > seqid

        resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
        assert new_seqid == resp.json()["last_seqid"]

        seqid = new_seqid


@pytest.mark.asyncio
async def test_last_seqid_updated_on_put_fields(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/resources",
        headers={"X-SYNCHRONOUS": "True"},
        json={
            "title": "My resource",
            "slug": "resource",
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]
    seqid = data["seqid"]

    tests = [
        ("text/text1", {"body": "Some text", "format": "PLAIN"}),
    ]

    for endpoint, payload in tests:
        resp = await nucliadb_writer.put(
            f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}/{endpoint}",
            json=payload,
        )
        assert resp.status_code == 200
        new_seqid = resp.json()["last_seqid"]
        assert new_seqid > seqid

        seqid = new_seqid
