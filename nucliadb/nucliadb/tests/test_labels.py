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
async def test_labels_global(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    # PUBLIC API
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}")
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/labelset/label1",
        json={
            "title": "mylabel",
            "labels": [{"title": "label1", "uri": "http://"}],
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/labelsets")
    assert resp.status_code == 200
    assert len(resp.json()["labelsets"]) == 1
