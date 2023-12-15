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
import io
import json
from unittest import mock

import pytest
from httpx import AsyncClient

from nucliadb.search.api.v1.chat import SyncChatResponse
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.utilities import get_predict


@pytest.fixture(scope="function", autouse=True)
def audit():
    audit_mock = mock.Mock(chat=mock.AsyncMock())
    with mock.patch(
        "nucliadb.search.search.chat.query.get_audit", return_value=audit_mock
    ):
        yield audit_mock


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat(
    nucliadb_reader: AsyncClient,
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
        "nucliadb.search.search.chat.query.find", return_value=(mock.MagicMock(), True)
    ):
        yield


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_handles_incomplete_find_results(
    nucliadb_reader: AsyncClient,
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


@pytest.fixture
async def resource(nucliadb_writer, knowledgebox):
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
    yield rid


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_handles_status_codes_in_a_different_chunk(
    nucliadb_reader: AsyncClient, knowledgebox, resource
):
    predict = get_predict()
    predict.generated_answer = [b"some ", b"text ", b"with ", b"status.", b"-2"]  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat", json={"query": "title"}
    )
    assert resp.status_code == 200
    _, answer, _, _ = parse_chat_response(resp.content)

    assert answer == b"some text with status."


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_handles_status_codes_in_the_same_chunk(
    nucliadb_reader: AsyncClient, knowledgebox, resource
):
    predict = get_predict()
    predict.generated_answer = [b"some ", b"text ", b"with ", b"status.-2"]  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat", json={"query": "title"}
    )
    assert resp.status_code == 200
    _, answer, _, _ = parse_chat_response(resp.content)

    assert answer == b"some text with status."


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_handles_status_codes_with_last_chunk_empty(
    nucliadb_reader: AsyncClient, knowledgebox, resource
):
    predict = get_predict()
    predict.generated_answer = [b"some ", b"text ", b"with ", b"status.", b"-2", b""]  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat", json={"query": "title"}
    )
    assert resp.status_code == 200
    _, answer, _, _ = parse_chat_response(resp.content)

    assert answer == b"some text with status."


def parse_chat_response(content: bytes):
    raw = io.BytesIO(content)
    header = raw.read(4)
    payload_size = int.from_bytes(header, byteorder="big", signed=False)
    data = raw.read(payload_size)
    find_result = json.loads(base64.b64decode(data))
    data = raw.read()
    try:
        answer, relations_payload = data.split(b"_END_")
    except ValueError:
        answer = data
        relations_payload = b""
    relations_result = None
    if len(relations_payload) > 0:
        relations_result = json.loads(base64.b64decode(relations_payload))
    try:
        answer, tail = answer.split(b"_CIT_")
        citations_length = int.from_bytes(tail[:4], byteorder="big", signed=False)
        citations_part = tail[4 : 4 + citations_length]
        citations = json.loads(base64.b64decode(citations_part).decode())
    except ValueError:
        answer = answer
        citations = {}
    return find_result, answer, relations_result, citations


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_always_returns_relations(
    nucliadb_reader: AsyncClient, knowledgebox
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat", json={"query": "summary", "features": ["relations"]}
    )
    assert resp.status_code == 200
    _, answer, relations_result, _ = parse_chat_response(resp.content)
    assert answer == b"Not enough data to answer this."
    assert "Ferran" in relations_result["entities"]


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_synchronous(nucliadb_reader: AsyncClient, knowledgebox, resource):
    predict = get_predict()
    predict.generated_answer = [b"some ", b"text ", b"with ", b"status.", b"0"]  # type: ignore
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncChatResponse.parse_raw(resp.content)

    assert resp_data.answer == "some text with status."
    assert len(resp_data.results.resources) == 1
    assert resp_data.status == AnswerStatusCode.SUCCESS


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
@pytest.mark.parametrize("sync_chat", (True, False))
async def test_chat_with_citations(
    nucliadb_reader: AsyncClient, knowledgebox, resource, sync_chat
):
    citations = {"foo": [], "bar": []}  # type: ignore
    citations_payload = base64.b64encode(json.dumps(citations).encode())
    citations_size = len(citations_payload).to_bytes(4, byteorder="big", signed=False)

    predict = get_predict()
    predict.generated_answer = [  # type: ignore
        b"some ",
        b"text ",
        b"with ",
        b"status.",
        b"_CIT_",
        citations_size,
        citations_payload,
        b"0",
    ]

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={"query": "title", "citations": True},
        headers={"X-Synchronous": str(sync_chat)},
        timeout=None,
    )
    assert resp.status_code == 200

    if sync_chat:
        resp_data = SyncChatResponse.parse_raw(resp.content)
        resp_citations = resp_data.citations
    else:
        resp_citations = parse_chat_response(resp.content)[-1]
    assert resp_citations == citations


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
@pytest.mark.parametrize("sync_chat", (True, False))
async def test_chat_without_citations(
    nucliadb_reader: AsyncClient, knowledgebox, resource, sync_chat
):
    predict = get_predict()
    predict.generated_answer = [  # type: ignore
        b"some ",
        b"text ",
        b"with ",
        b"status.",
        b"0",
    ]

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={"query": "title", "citations": False},
        headers={"X-Synchronous": str(sync_chat)},
        timeout=None,
    )
    assert resp.status_code == 200

    if sync_chat:
        resp_data = SyncChatResponse.parse_raw(resp.content)
        resp_citations = resp_data.citations
    else:
        resp_citations = parse_chat_response(resp.content)[-1]
    assert resp_citations == {}
