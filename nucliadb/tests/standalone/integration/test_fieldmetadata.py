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

from nucliadb.search.api.v1.router import KB_PREFIX


@pytest.mark.deploy_modes("standalone")
async def test_fieldmetadata_crud(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox_one: str,
) -> None:
    """Test description:

    1. Create a resource with a couple of text fields and some initial
      fieldmetadata for the first field

    2. Add metadata for the seconds field using PATCH

    3. Overwrite first field metadata using PATCH

    """

    fieldmetadata_0 = {
        "field": {"field": "textfield1", "field_type": "text"},
        "question_answers": [],
        "paragraphs": [
            {
                "key": "paragraph0",
                "classifications": [
                    {
                        "labelset": "My Labels",
                        "label": "Label 0",
                        "cancelled_by_user": False,
                    },
                ],
            },
        ],
    }
    fieldmetadata_1 = {
        "field": {"field": "textfield2", "field_type": "text"},
        "question_answers": [],
        "paragraphs": [
            {
                "key": "paragraph2",
                "classifications": [
                    {
                        "labelset": "My Labels",
                        "label": "Label 2",
                        "cancelled_by_user": False,
                    }
                ],
            }
        ],
    }
    fieldmetadata_2 = {
        "field": {"field": "textfield1", "field_type": "text"},
        "paragraphs": [
            {
                "key": "paragraph1",
                "classifications": [
                    {
                        "labelset": "My Labels",
                        "label": "Label 1",
                        "cancelled_by_user": True,
                    }
                ],
            }
        ],
        "question_answers": [],
    }

    # Step 1

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_one}/resources",
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

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}")
    fieldmetadata = resp.json()["fieldmetadata"]
    assert len(fieldmetadata) == 1
    assert fieldmetadata[0] == fieldmetadata_0

    # Step 2

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}",
        json={"fieldmetadata": [fieldmetadata_1]},
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}?show=basic&show=extracted",
    )
    assert resp.status_code == 200
    fieldmetadata = resp.json()["fieldmetadata"]

    assert len(fieldmetadata) == 2
    assert fieldmetadata[0] == fieldmetadata_0
    assert fieldmetadata[1]["field"] == fieldmetadata_1["field"]
    assert fieldmetadata[1]["paragraphs"] == fieldmetadata_1["paragraphs"]

    # Step 3

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}",
        json={"fieldmetadata": [fieldmetadata_2]},
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}?show=basic&show=extracted",
    )
    assert resp.status_code == 200
    fieldmetadata = resp.json()["fieldmetadata"]

    assert len(fieldmetadata) == 2
    assert fieldmetadata[0] == fieldmetadata_2
    assert fieldmetadata[1]["field"] == fieldmetadata_1["field"]
    assert fieldmetadata[1]["paragraphs"] == fieldmetadata_1["paragraphs"]
