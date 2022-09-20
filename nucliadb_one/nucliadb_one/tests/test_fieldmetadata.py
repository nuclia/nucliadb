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
    # Create a resource with a couple of text fields
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/resources",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "texts": {
                    "textfield1": {"body": "Some text", "format": "PLAIN"},
                    "textfield2": {"body": "Some other text", "format": "PLAIN"},
                }
            },
        )
        assert resp.status_code == 201
        resp = resp.json()
        uuid = resp["uuid"]

    # Add metadata for the first field
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.patch(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "fieldmetadata": [
                    {
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
                ]
            },
        )
        assert resp.status_code == 200

    # Check that fieldmetadata was correctly saved
    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}?show=basic",
        )
        assert resp.status_code == 200
        data = resp.json()

        assert data["fieldmetadata"] == [
            {
                "field": {"field": "textfield1", "field_type": "text"},
                "paragraphs": [
                    {
                        "key": "paragraph1",
                        "classification": [
                            {"labelset": "My Labels", "label": "Label 1"}
                        ],
                    }
                ],
            }
        ]

    # Add metadata for second field
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.patch(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "fieldmetadata": [
                    {
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
                ]
            },
        )
        assert resp.status_code == 200

    # Check that fieldmedatada on second field was saved and also preserved on first field
    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{knowledgebox_one}/resource/{uuid}?show=basic",
        )
        assert resp.status_code == 200
        data = resp.json()

        # Check that fieldmetadata is correct
        assert data["fieldmetadata"] == [
            {
                "field": {"field": "textfield1", "field_type": "text"},
                "paragraphs": [
                    {
                        "key": "paragraph1",
                        "classification": [
                            {"labelset": "My Labels", "label": "Label 1"}
                        ],
                    }
                ],
            },
            {
                "field": {"field": "textfield2", "field_type": "text"},
                "paragraphs": [
                    {
                        "key": "paragraph2",
                        "classification": [
                            {"labelset": "My Labels", "label": "Label 2"}
                        ],
                    }
                ],
            },
        ]
