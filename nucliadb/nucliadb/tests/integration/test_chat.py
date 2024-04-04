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
        "nucliadb.search.search.chat.query.find",
        return_value=(mock.MagicMock(), True, None),
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
        f"/kb/{knowledgebox}/chat",
        json={"query": "summary", "features": ["relations"]},
        timeout=None,
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


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
@pytest.mark.parametrize("debug", (True, False))
async def test_sync_chat_returns_prompt_context(
    nucliadb_reader: AsyncClient, knowledgebox, resource, debug
):
    # Make sure prompt context is returned if debug is True
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={"query": "title", "debug": debug},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncChatResponse.parse_raw(resp.content)
    if debug:
        assert resp_data.prompt_context
        assert resp_data.prompt_context_order
    else:
        assert resp_data.prompt_context is None
        assert resp_data.prompt_context_order is None


@pytest.fixture
async def resources(nucliadb_writer, knowledgebox):
    kbid = knowledgebox
    rids = []
    for i in range(2):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": f"The title {i}",
                "summary": f"The summary {i}",
                "texts": {"text_field": {"body": "The body of the text field"}},
            },
        )
        assert resp.status_code in (200, 201)
        rid = resp.json()["uuid"]
        rids.append(rid)
    yield rids


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_rag_options_full_resource(
    nucliadb_reader: AsyncClient, knowledgebox, resources
):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={"query": "title", "rag_strategies": [{"name": "full_resource"}]},
        timeout=None,
    )
    assert resp.status_code == 200
    _ = parse_chat_response(resp.content)

    # Make sure the prompt context is properly crafted
    assert predict.calls[-2][0] == "chat_query"  # type: ignore
    prompt_context = predict.calls[-2][1].query_context  # type: ignore

    # All fields of the matching resource should be in the prompt context
    assert len(prompt_context) == 6
    assert prompt_context[f"{resource1}/a/title"] == "The title 0"
    assert prompt_context[f"{resource1}/a/summary"] == "The summary 0"
    assert prompt_context[f"{resource1}/t/text_field"] == "The body of the text field"
    assert prompt_context[f"{resource2}/a/title"] == "The title 1"
    assert prompt_context[f"{resource2}/a/summary"] == "The summary 1"
    assert prompt_context[f"{resource2}/t/text_field"] == "The body of the text field"


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_rag_options_extend_with_fields(
    nucliadb_reader: AsyncClient, knowledgebox, resources
):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={
            "query": "title",
            "rag_strategies": [{"name": "field_extension", "fields": ["a/summary"]}],
        },
        timeout=None,
    )
    assert resp.status_code == 200
    _ = parse_chat_response(resp.content)

    # Make sure the prompt context is properly crafted
    assert predict.calls[-2][0] == "chat_query"  # type: ignore
    prompt_context = predict.calls[-2][1].query_context  # type: ignore

    # Matching paragraphs should be in the prompt
    # context, plus the extended field for each resource
    assert len(prompt_context) == 4
    # The matching paragraphs
    assert prompt_context[f"{resource1}/a/title/0-11"] == "The title 0"
    assert prompt_context[f"{resource2}/a/title/0-11"] == "The title 1"
    # The extended fields
    assert prompt_context[f"{resource1}/a/summary"] == "The summary 0"
    assert prompt_context[f"{resource2}/a/summary"] == "The summary 1"


@pytest.mark.asyncio()
async def test_chat_rag_options_validation(nucliadb_reader):
    # Invalid strategy
    resp = await nucliadb_reader.post(
        f"/kb/kbid/chat",
        json={
            "query": "title",
            "rag_strategies": [{"name": "foobar", "fields": ["a/summary"]}],
        },
    )
    assert resp.status_code == 422

    # Invalid strategy as a string
    resp = await nucliadb_reader.post(
        f"/kb/kbid/chat",
        json={
            "query": "title",
            "rag_strategies": ["full_resource"],
        },
    )
    assert resp.status_code == 422

    # Invalid strategy without name
    resp = await nucliadb_reader.post(
        f"/kb/kbid/chat",
        json={
            "query": "title",
            "rag_strategies": [{"fields": ["a/summary"]}],
        },
    )
    assert resp.status_code == 422

    # full_resource cannot be combined with other strategies
    resp = await nucliadb_reader.post(
        f"/kb/kbid/chat",
        json={
            "query": "title",
            "rag_strategies": [
                {"name": "full_resource"},
                {"name": "field_extension", "fields": ["a/summary"]},
            ],
        },
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    assert (
        detail[0]["msg"]
        == "If 'full_resource' strategy is chosen, it must be the only strategy"
    )

    # field_extension requires fields
    resp = await nucliadb_reader.post(
        f"/kb/kbid/chat",
        json={"query": "title", "rag_strategies": [{"name": "field_extension"}]},
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    detail[0]["loc"][-1] == "fields"
    assert detail[0]["msg"] == "field required"

    # fields must be in the right format: field_type/field_name
    resp = await nucliadb_reader.post(
        f"/kb/kbid/chat",
        json={
            "query": "title",
            "rag_strategies": [{"name": "field_extension", "fields": ["foo/t/text"]}],
        },
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    detail[0]["loc"][-1] == "fields"
    assert (
        detail[0]["msg"]
        == "Field 'foo/t/text' is not in the format {field_type}/{field_name}"
    )

    # But fields can have leading and trailing slashes and they will be ignored
    resp = await nucliadb_reader.post(
        f"/kb/foo/chat",
        json={
            "query": "title",
            "rag_strategies": [{"name": "field_extension", "fields": ["/a/text/"]}],
        },
    )
    assert resp.status_code != 422

    # fields must have a valid field type
    resp = await nucliadb_reader.post(
        f"/kb/kbid/chat",
        json={
            "query": "title",
            "rag_strategies": [{"name": "field_extension", "fields": ["X/fieldname"]}],
        },
    )
    assert resp.status_code == 422
    detail = resp.json()["detail"]
    detail[0]["loc"][-1] == "fields"
    assert detail[0]["msg"].startswith(
        "Field 'X/fieldname' does not have a valid field type. Valid field types are"
    )


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_chat_capped_context(
    nucliadb_reader: AsyncClient, knowledgebox, resources
):
    # By default, max size is big enough to fit all the prompt context
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={
            "query": "title",
            "rag_strategies": [{"name": "full_resource"}],
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
        timeout=None,
    )
    assert resp.status_code == 200
    resp_data = SyncChatResponse.parse_raw(resp.content)
    assert resp_data.prompt_context is not None
    assert len(resp_data.prompt_context) == 6
    total_size = sum(len(v) for v in resp_data.prompt_context.values())
    # Try now setting a smaller max size. It should be respected
    max_size = 28
    assert total_size > max_size * 3

    predict = get_predict()
    predict.max_context = max_size  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/chat",
        json={
            "query": "title",
            "rag_strategies": [{"name": "full_resource"}],
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
        timeout=None,
    )
    assert resp.status_code == 200, resp.text
    resp_data = SyncChatResponse.parse_raw(resp.content)
    assert resp_data.prompt_context is not None
    total_size = sum(len(v) for v in resp_data.prompt_context.values())
    assert total_size <= max_size * 3


@pytest.mark.asyncio()
async def test_chat_on_a_kb_not_found(nucliadb_reader):
    resp = await nucliadb_reader.post("/kb/unknown_kb_id/chat", json={"query": "title"})
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Knowledge Box 'unknown_kb_id' not found."}
