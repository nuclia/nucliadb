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


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_counters(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    # PUBLIC API
    resp = await nucliadb_reader.get(f"/v1/kb/{knowledgebox}")
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/v1/kb/{knowledgebox}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "usermetadata": {
                "classifications": [{"labelset": "type", "label": "Book"}]
            },
        },
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        f"/v1/kb/{knowledgebox}/resources",
        json={"slug": "myresource2", "title": "mytitle1"},
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        f"/v1/kb/{knowledgebox}/resources",
        json={"slug": "myresource3", "title": "mytitle1"},
    )
    assert resp.status_code == 201

    resp = await nucliadb_reader.get(f"/v1/kb/{knowledgebox}/counters")

    assert resp.status_code == 200
    assert resp.json()["resources"] == 3
    assert resp.json()["paragraphs"] == 3
    assert resp.json()["fields"] == 3
    assert resp.json()["index_size"] == 30_000
