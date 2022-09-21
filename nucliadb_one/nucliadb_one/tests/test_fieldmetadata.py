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
    """Test description:

    - Create a resource with a couple of text fields and some initial
      fieldmetadata

    - Validate fieldmetadata is properly set and returned

    - Add more metadata using a PATCH request

    - Validate all fieldmetadata is merged and returned

    """

    fieldmetadata_0 = {
        "field": {"field": "textfield1", "field_type": "text"},
        "paragraphs": [
            {
                "key": "paragraph0",
                "classifications": [
                    {"labelset": "My Labels", "label": "Label 0"},
                ],
            },
        ],
    }
    fieldmetadata_1 = {
        "field": {"field": "textfield1", "field_type": "text"},
        "paragraphs": [
            {
                "key": "paragraph1",
                "classifications": [
                    {"labelset": "My Labels", "label": "Label 1"}
                ],
            }
        ],
    }
    fieldmetadata_2 = {
        "field": {"field": "textfield2", "field_type": "text"},
        "paragraphs": [
            {
                "key": "paragraph2",
                "classifications": [
                    {"labelset": "My Labels", "label": "Label 2"}
                ],
            }
        ],
    }

    # Create a resource with a couple of text fields and some fieldmetadata
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/resources",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "texts": {
                    "textfield1": {"body": "Some text", "format": "PLAIN"},
                    "textfield2": {"body": "Some other text", "format": "PLAIN"},
                },
                "fieldmetadata": [fieldmetadata_0],
            },
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}")
        fieldmetadata = resp.json()["fieldmetadata"]
        assert len(fieldmetadata) == 1
        assert fieldmetadata[0]["field"] == fieldmetadata_0["field"]
        assert fieldmetadata[0]["paragraphs"] == fieldmetadata_0["paragraphs"]

    # Add more metadata on resource modification
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.patch(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}",
            headers={"X-SYNCHRONOUS": "True"},
            json={"fieldmetadata": [fieldmetadata_1, fieldmetadata_2]},
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}?show=basic&show=extracted",
        )
        assert resp.status_code == 200
        fieldmetadata = resp.json()["fieldmetadata"]

        assert len(fieldmetadata) == 3
        assert fieldmetadata[0]["field"] == fieldmetadata_0["field"]
        assert fieldmetadata[0]["paragraphs"] == fieldmetadata_0["paragraphs"]
        assert fieldmetadata[1]["field"] == fieldmetadata_1["field"]
        assert fieldmetadata[1]["paragraphs"] == fieldmetadata_1["paragraphs"]
        assert fieldmetadata[2]["field"] == fieldmetadata_2["field"]
        assert fieldmetadata[2]["paragraphs"] == fieldmetadata_2["paragraphs"]


@pytest.mark.asyncio
async def test_delete_fieldmetadata(
    nucliadb_api: Callable[..., AsyncClient], knowledgebox_one
) -> None:

    fieldmetadata_0 = {
        "field": {"field": "textfield1", "field_type": "text"},
        "paragraphs": [
            {
                "key": "paragraph2",
                "classifications": [
                    {"labelset": "My Labels", "label": "Label 2"}
                ],
            }
        ],
    }

    fieldmetadata_delete = {
        "field": {"field": "textfield1", "field_type": "text"},
        "paragraphs": []
    }

    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/resources",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "texts": {
                    "textfield1": {"body": "Some text", "format": "PLAIN"},
                    "textfield2": {"body": "Some other text", "format": "PLAIN"},
                },
                "fieldmetadata": [fieldmetadata_0],
            },
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]

        resp = await client.patch(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}",
            headers={"X-SYNCHRONOUS": "True"},
            json={"fieldmetadata": [fieldmetadata_delete]},
        )
        assert resp.status_code == 200

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}?show=basic&show=extracted",
        )
        assert resp.status_code == 200
        fieldmetadata = resp.json()["fieldmetadata"]

        assert len(fieldmetadata) == 1
        assert fieldmetadata[0]["field"]["field"] == "textfield1"
        assert len(fieldmetadata[0]["paragraphs"]) == 0
