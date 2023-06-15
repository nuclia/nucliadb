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
from unittest import mock

import pytest


@pytest.mark.asyncio()
async def test_chat(
    nucliadb_reader,
    knowledgebox,
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat", json={"query": "query"}
    )
    assert resp.status_code == 200

    context = [{"author": "USER", "text": "query"}]
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat", json={"query": "query", "context": context}
    )
    assert resp.status_code == 200


@pytest.fixture(scope="function")
def find_incomplete_results():
    with mock.patch(
        "nucliadb.search.api.v1.chat.find", return_value=(mock.MagicMock(), True)
    ):
        yield


@pytest.mark.asyncio()
async def test_chat_handles_incomplete_find_results(
    nucliadb_reader,
    knowledgebox,
    find_incomplete_results,
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat", json={"query": "query"}
    )
    assert resp.status_code == 529
    assert resp.json() == {
        "detail": "Temporary error on information retrieval. Please try again."
    }
