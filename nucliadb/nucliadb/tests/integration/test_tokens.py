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
async def test_metadata_tokens_cancelled_by_the_user_sc_3775(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    token = {
        "token": "DRAG",
        "start": 340,
        "end": 344,
        "klass": "WORK_OF_ART",
        "cancelled_by_user": True,
    }
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"X-Synchronous": "true"},
        json={
            "title": "My Resource",
            "summary": "My summary",
            "fieldmetadata": [
                {
                    "token": [token],
                    "paragraphs": [],
                    "field": {
                        "field_type": "file",
                        "field": "20fd69d4b4dcdf0eb9e8c95dfff1ce6c",
                    },
                }
            ],
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Check cancelled tokens come in resource get
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}",
    )
    assert resp.status_code == 200
    content = resp.json()
    assert content["fieldmetadata"][0]["token"][0] == token

    # Check cancelled labels come in resource list
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resources",
    )
    assert resp.status_code == 200
    content = resp.json()
    assert content["resources"][0]["fieldmetadata"][0]["token"][0] == token

    # Check cancelled labels come in search results
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=summary")
    assert resp.status_code == 200
    content = resp.json()
    assert content["resources"][rid]["fieldmetadata"][0]["token"][0] == token
