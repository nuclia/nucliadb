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
import base64
import json
import os

import pytest
from httpx import AsyncClient


@pytest.fixture(scope="function")
@pytest.mark.asyncio
async def annotated_file_field(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox
    field_id = "invoice"

    testdata_dir = os.path.join(
        os.path.dirname(__file__),
        "..",
        "testdata",
    )

    invoice_filename = os.path.join(testdata_dir, "invoice.pdf")
    with open(invoice_filename, "rb") as f:
        invoice_content = f.read()

    selections_filename = os.path.join(testdata_dir, "invoice_selections.json")
    with open(selections_filename) as f:
        selections = json.load(f)

    selections_by_page = {}
    for annotation in selections["annotations"]:
        page_selections = selections_by_page.setdefault(annotation["page"], [])
        page_selections.append(
            {
                "label": annotation["label"]["text"],
                "top": annotation["bounds"]["top"],
                "left": annotation["bounds"]["left"],
                "right": annotation["bounds"]["right"],
                "bottom": annotation["bounds"]["bottom"],
                "token_ids": [
                    # TODO pageIndex?
                    token["tokenIndex"]
                    for token in annotation["tokens"]
                ],
            }
        )

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        headers={"x-synchronous": "true"},
        json={
            "title": "My invoice",
            "files": {
                field_id: {
                    "file": {
                        "filename": "invoice.pdf",
                        "content_type": "application/pdf",
                        "payload": base64.b64encode(invoice_content).decode(),
                    }
                }
            },
            "fieldmetadata": [
                {
                    "field": {"field": field_id, "field_type": "file"},
                    "selections": [
                        {
                            "page": page,
                            "visual": selections,
                        }
                        for page, selections in selections_by_page.items()
                    ],
                }
            ],
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    yield (rid, field_id)


@pytest.mark.asyncio
async def test_visual_selection(
    nucliadb_reader: AsyncClient, knowledgebox: str, annotated_file_field
):
    kbid = knowledgebox
    rid, field_id = annotated_file_field

    resp = await nucliadb_reader.get(
        # f"/kb/{kbid}/resource/{rid}/file/{field_id}",
        # params={"show": ["value", "extracted"]},
        f"/kb/{kbid}/resource/{rid}",
        params={"show": ["basic", "extra", "values", "extracted"]},
    )
    assert resp.status_code == 200
    body = resp.json()

    # TODO
    pass
