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
import json

import pytest
from httpx import AsyncClient

from nucliadb_models.text import TextFormat


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_text_field_in_json_format(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox
    field_id = "json-text"
    payload = {"hello": "world"}

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "JSON text field",
            "texts": {
                field_id: {
                    "body": json.dumps(payload),
                    "format": TextFormat.JSON.value,
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}?show=values")
    assert resp.status_code == 200

    body = resp.json()
    assert json.loads(body["data"]["texts"][field_id]["value"]["body"]) == payload


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_text_field_with_invalid_json(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox
    field_id = "json-text"
    invalid_json = '{hello": "world"}'

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "JSON text field",
            "texts": {
                field_id: {
                    "body": invalid_json,
                    "format": TextFormat.JSON.value,
                }
            },
        },
    )
    assert resp.status_code == 422
