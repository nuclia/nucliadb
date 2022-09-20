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
from typing import Callable

import pytest
from httpx import AsyncClient

from nucliadb_models.resource import NucliaDBRoles
from nucliadb_search.api.v1.router import KB_PREFIX

# TODO
# - Patch one field. Make sure other fields are intact.
# - Delete metadata of one field.


@pytest.mark.asyncio
async def test_patch_fieldmetadata(
    nucliadb_api: Callable[..., AsyncClient], knowledgebox_one
) -> None:

    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/resources",
            headers={"X-SYNCHRONOUS": "True"},
            json={"texts": {"text1": {"body": "test1", "format": "PLAIN"}}},
        )
        assert resp.status_code == 201
        resp = resp.json()
        uuid = resp["uuid"]

        resp = await client.patch(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "fieldmetadata": [
                    {
                        "field": {"field": "text1", "field_type": "text"},
                        "paragraphs": [
                            {
                                "key": "paragraph1",
                                "classifications": [
                                    {"labelset": "My Labels", "label": "Label 1"}
                                ],
                            }
                        ],
                    }
                ]
            },
        )
        assert resp.status_code == 200

        resp = await client.patch(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "fieldmetadata": [
                    {
                        "field": {"field": "text1", "field_type": "text"},
                        "paragraphs": [
                            {
                                "key": "paragraph2",
                                "classifications": [
                                    {"labelset": "My Labels", "label": "Label 2"}
                                ],
                            }
                        ],
                    }
                ]
            },
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp1 = await client.get(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}?show=basic",
        )
        assert resp1.status_code == 200
    data = resp1.json()

    assert (
        data["fieldmetadata"][0]["paragraphs"][0]["classifications"][0]["label"]
        == "Label 1"
    )
