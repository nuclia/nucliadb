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
from unittest import mock

import nats
import pytest
from httpx import AsyncClient
from nats.aio.client import Client
from nats.js import JetStreamContext

from nucliadb.search.predict import (
    AnswerStatusCode,
    CitationsGenerativeResponse,
    GenerativeChunk,
    StatusGenerativeResponse,
)
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import AskResponseItem, SyncAskResponse
from nucliadb_protos.audit_pb2 import AuditRequest
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.utilities import Utility, set_utility


@pytest.fixture(scope="function", autouse=True)
def audit(request):
    if "skip_audit_mock" in request.keywords:
        yield
    else:
        audit_mock = mock.Mock(chat=mock.AsyncMock())
        with mock.patch("nucliadb.search.search.chat.query.get_audit", return_value=audit_mock):
            yield audit_mock


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 200

    context = [{"author": "USER", "text": "query"}]
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "query",
            "context": context,
            "answer_json_schema": {
                "type": "object",
                "properties": {"answer": {"type": "string"}, "confidence": {"type": "number"}},
            },
        },
    )
    assert resp.status_code == 200


async def get_audit_messages(sub):
    msg = await sub.fetch(1)
    auditreq = AuditRequest()
    auditreq.ParseFromString(msg[0].data)
    return auditreq


@pytest.fixture(scope="function")
def find_incomplete_results():
    with mock.patch(
        "nucliadb.search.search.chat.query.find",
        return_value=(mock.MagicMock(), True, None),
    ):
        yield


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_handles_incomplete_find_results(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    find_incomplete_results,
):
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 529
    assert resp.json() == {"detail": "Temporary error on information retrieval. Please try again."}


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


@pytest.mark.skip_audit_mock
@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_sends_one_combined_audit(
    nucliadb_reader: AsyncClient, knowledgebox, stream_audit: StreamAuditStorage, resource
):
    from nucliadb_utils.settings import audit_settings

    kbid = knowledgebox

    predict = get_predict()
    predict.generated_answer = [b"some ", b"text ", b"with ", b"status.", b"-2"]  # type: ignore

    # Prepare a test audit stream to receive our messages
    partition = stream_audit.get_partition(kbid)
    client: Client = await nats.connect(stream_audit.nats_servers)
    jetstream: JetStreamContext = client.jetstream()
    if audit_settings.audit_jetstream_target is None:
        assert False, "Missing jetstream target in audit settings"
    subject = audit_settings.audit_jetstream_target.format(partition=partition, type="*")

    set_utility(Utility.AUDIT, stream_audit)

    try:
        await jetstream.delete_stream(name=audit_settings.audit_stream)
        await jetstream.delete_stream(name="test_usage")
    except nats.js.errors.NotFoundError:
        pass

    await jetstream.add_stream(name=audit_settings.audit_stream, subjects=[subject])

    psub = await jetstream.pull_subscribe(subject, "psub")

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.answer == "valid answer to"
    assert len(resp_data.retrieval_results.resources) == 1

    auditreq = await get_audit_messages(psub)
    assert auditreq.kbid == kbid
    assert auditreq.HasField("chat")
    assert auditreq.HasField("search")
    assert auditreq.request_time > 0
    assert auditreq.generative_answer_time > 0
    assert auditreq.retrieval_time > 0
    assert (auditreq.generative_answer_time + auditreq.retrieval_time) < auditreq.request_time
    try:
        auditreq = await get_audit_messages(psub)
    except nats.errors.TimeoutError:
        pass
    else:
        assert "There was an unexpected extra audit message in nats"
    await psub.unsubscribe()
    await client.flush()
    await client.close()


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_synchronous(nucliadb_reader: AsyncClient, knowledgebox, resource):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.answer == "valid answer to"
    assert len(resp_data.retrieval_results.resources) == 1
    assert resp_data.status == AnswerStatusCode.SUCCESS.prettify()


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_with_citations(nucliadb_reader: AsyncClient, knowledgebox, resource):
    citations = {"foo": [], "bar": []}  # type: ignore
    citations_gen = CitationsGenerativeResponse(citations=citations)
    citations_chunk = GenerativeChunk(chunk=citations_gen)

    predict = get_predict()
    predict.ndjson_answer.append(citations_chunk.model_dump_json() + "\n")  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title", "citations": True},
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 200

    resp_data = SyncAskResponse.model_validate_json(resp.content)
    resp_citations = resp_data.citations
    assert resp_citations == citations


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
@pytest.mark.parametrize("debug", (True, False))
async def test_sync_ask_returns_prompt_context(
    nucliadb_reader: AsyncClient, knowledgebox, resource, debug
):
    # Make sure prompt context is returned if debug is True
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title", "debug": debug},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    if debug:
        assert resp_data.prompt_context
    else:
        assert resp_data.prompt_context is None


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


def parse_ask_response(resp):
    results = []
    for line in resp.iter_lines():
        result_item = AskResponseItem.model_validate_json(line)
        results.append(result_item)
    return results


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_rag_options_full_resource(nucliadb_reader: AsyncClient, knowledgebox, resources):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title", "rag_strategies": [{"name": "full_resource"}]},
    )
    assert resp.status_code == 200
    _ = parse_ask_response(resp)

    # Make sure the prompt context is properly crafted
    assert predict.calls[-2][0] == "chat_query_ndjson"  # type: ignore
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
async def test_ask_rag_options_extend_with_fields(nucliadb_reader: AsyncClient, knowledgebox, resources):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "title",
            "rag_strategies": [{"name": "field_extension", "fields": ["a/summary"]}],
        },
    )
    assert resp.status_code == 200
    _ = parse_ask_response(resp)

    # Make sure the prompt context is properly crafted
    assert predict.calls[-2][0] == "chat_query_ndjson"  # type: ignore
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
@pytest.mark.parametrize(
    "invalid_payload,expected_error_msg",
    [
        (
            # Invalid strategy type
            {
                "query": "title",
                "rag_strategies": [{"name": "foobar", "fields": ["a/summary"]}],
            },
            None,
        ),
        (
            # Invalid strategy without name
            {
                "query": "title",
                "rag_strategies": [{"fields": ["a/summary"]}],
            },
            None,
        ),
        (
            # full_resource cannot be combined with other strategies
            {
                "query": "title",
                "rag_strategies": [
                    {"name": "full_resource"},
                    {"name": "field_extension", "fields": ["a/summary"]},
                ],
            },
            "If 'full_resource' strategy is chosen, it must be the only strategy",
        ),
        (
            # field_extension requires fields
            {
                "query": "title",
                "rag_strategies": [{"name": "field_extension"}],
            },
            "Field required",
        ),
        (
            # fields must be in the right format: field_type/field_name
            {
                "query": "title",
                "rag_strategies": [{"name": "field_extension", "fields": ["foo/t/text"]}],
            },
            "Value error, Field 'foo/t/text' is not in the format {field_type}/{field_name}",
        ),
        (
            # fields must have a valid field type
            {
                "query": "title",
                "rag_strategies": [{"name": "field_extension", "fields": ["X/fieldname"]}],
            },
            "Value error, Field 'X/fieldname' does not have a valid field type. Valid field types are",
        ),
        (
            # Invalid list type
            {
                "query": "title",
                "rag_strategies": ["foo"],
            },
            "must be defined using an object",
        ),
        (
            # Invalid payload type (note the extra json.dumps)
            json.dumps(
                {
                    "query": "title",
                    "rag_strategies": ["foo"],
                }
            ),
            None,
        ),
    ],
)
async def test_ask_rag_strategies_validation(nucliadb_reader, invalid_payload, expected_error_msg):
    # Invalid strategy as a string
    resp = await nucliadb_reader.post(
        f"/kb/kbid/ask",
        json=invalid_payload,
    )
    assert resp.status_code == 422
    if expected_error_msg:
        error_msg = resp.json()["detail"][0]["msg"]
        assert expected_error_msg in error_msg


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_capped_context(nucliadb_reader: AsyncClient, knowledgebox, resources):
    # By default, max size is big enough to fit all the prompt context
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "title",
            "rag_strategies": [{"name": "full_resource"}],
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.prompt_context is not None
    assert len(resp_data.prompt_context) == 6
    total_size = sum(len(v) for v in resp_data.prompt_context)
    # Try now setting a smaller max size. It should be respected
    max_size = 28
    assert total_size > max_size * 3

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "title",
            "rag_strategies": [{"name": "full_resource"}],
            "debug": True,
            "max_tokens": {"context": max_size},
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.prompt_context is not None
    total_size = sum(len(v) for v in resp_data.prompt_context)
    assert total_size <= max_size * 3


@pytest.mark.asyncio()
async def test_ask_on_a_kb_not_found(nucliadb_reader):
    resp = await nucliadb_reader.post("/kb/unknown_kb_id/ask", json={"query": "title"})
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Knowledge Box not found."}


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_max_tokens(nucliadb_reader, knowledgebox, resources):
    # As an integer
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "title",
            "max_tokens": 100,
        },
    )
    assert resp.status_code == 200

    # Same but with the max tokens in a dict
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "title",
            "max_tokens": {"context": 100, "answer": 50},
        },
    )
    assert resp.status_code == 200

    # If the context requested is bigger than the max tokens, it should fail
    predict = get_predict()
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "title",
            "max_tokens": {"context": predict.max_context + 1},
        },
    )
    assert resp.status_code == 412


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_on_resource(nucliadb_reader: AsyncClient, knowledgebox, resource):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/resource/{resource}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    SyncAskResponse.model_validate_json(resp.content)


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_handles_stream_errors_on_predict(nucliadb_reader, knowledgebox, resource):
    predict = get_predict()
    prev = predict.ndjson_answer.copy()

    predict.ndjson_answer.pop(-1)
    error_status = StatusGenerativeResponse(code="-1", details="unexpected LLM error")
    status_chunk = GenerativeChunk(chunk=error_status)
    predict.ndjson_answer.append(status_chunk.model_dump_json() + "\n")

    # Sync ask
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    ask_resp = SyncAskResponse.model_validate_json(resp.content)
    assert ask_resp.status == AnswerStatusCode.ERROR.prettify()
    assert ask_resp.error_details == "unexpected LLM error"

    # Stream ask
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title"},
    )
    assert resp.status_code == 200
    results = parse_ask_response(resp)
    status_item = results[-1].item
    assert status_item.type == "status"
    assert status_item.status == AnswerStatusCode.ERROR.prettify()
    assert status_item.details == "unexpected LLM error"

    predict.ndjson_answer = prev


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ask_handles_stream_unexpected_errors(nucliadb_reader, knowledgebox, resource):
    with mock.patch(
        "nucliadb.search.search.chat.ask.AskResult._stream",
        side_effect=ValueError("foobar"),
    ):
        # Sync ask -- should return a 500
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/ask",
            json={"query": "title"},
            headers={"X-Synchronous": "True"},
        )
        assert resp.status_code == 500

        # Stream ask -- should handle by yielding the error item
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/ask",
            json={"query": "title"},
        )
        assert resp.status_code == 200
        results = parse_ask_response(resp)
        error_item = results[-1].item
        assert error_item.type == "error"
        assert (
            error_item.error == "Unexpected error while generating the answer. Please try again later."
        )
