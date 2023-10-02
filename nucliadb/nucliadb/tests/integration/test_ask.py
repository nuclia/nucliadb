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

import unittest

import pytest


@pytest.fixture(scope="function")
def feature_disabled():
    with unittest.mock.patch(
        "nucliadb.search.api.v1.resource.ask.has_feature", return_value=False
    ):
        yield


@pytest.mark.asyncio()
async def test_ask_document(
    nucliadb_writer,
    nucliadb_reader,
    knowledgebox,
):
    kbid = knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "The title",
            "summary": "The summary",
            "texts": {"text_field": {"body": "The body of the text field"}},
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code in (200, 201)
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/resource/{rid}/ask",
        json={"question": "Some question?"},
    )
    assert resp.status_code == 200
    assert resp.json()["answer"] == "Answer to your question"


@pytest.mark.asyncio()
async def test_ask_document_disabled(
    nucliadb_writer,
    nucliadb_reader,
    knowledgebox,
    feature_disabled,
):
    kbid = knowledgebox
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/resource/someresource/ask",
        json={"question": "Some question?"},
    )
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Feature not yet available"
