# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import asyncio
import inspect
import json
import time
from functools import partial
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nidx_protos.nodereader_pb2 import SearchRequest

from nucliadb_protos.audit_pb2 import AuditRequest, ChatContext, RetrievedContext
from nucliadb_utils.audit.stream import (
    AuditedEndpoint,
    RequestContext,
    StreamAuditStorage,
    request_context_var,
    valid_payload,
    valid_query_params,
)


@pytest.fixture()
def nats():
    mock = AsyncMock()
    mock.jetstream = MagicMock(return_value=AsyncMock())
    yield mock


@pytest.fixture()
async def audit_storage(nats):
    with patch("nucliadb_utils.audit.stream.nats.connect", return_value=nats):
        aud = StreamAuditStorage(
            nats_servers=["nats://localhost:4222"],
            nats_target="test",
            partitions=1,
            seed=1,
            nats_creds="nats_creds",
        )
        await aud.initialize()
        yield aud
        await aud.finalize()


def stream_audit_finish_condition(audit_storage: StreamAuditStorage, count_publish: int):
    return (
        audit_storage.queue.qsize() == 0
        and audit_storage.kb_usage_utility.queue.qsize() == 0
        and audit_storage.js.publish.call_count == count_publish  # type: ignore[ty:unresolved-attribute]
    )


async def wait_until(condition, timeout=1):
    start = time.monotonic()
    while True:
        result_or_coro = condition()
        if inspect.iscoroutine(result_or_coro):
            result = await result_or_coro
        else:
            result = result_or_coro

        if result:
            break

        await asyncio.sleep(0.05)
        if time.monotonic() - start > timeout:
            raise Exception("TESTING ERROR: Condition was never reached")


async def test_lifecycle(audit_storage: StreamAuditStorage, nats):
    nats.jetstream.assert_called_once()

    await audit_storage.finalize()
    nats.close.assert_called_once()


async def test_publish(audit_storage: StreamAuditStorage, nats):
    await audit_storage.initialize()
    audit_storage.send(AuditRequest())

    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_report(audit_storage: StreamAuditStorage, nats):
    audit_storage.report_and_send(kbid="kbid", audit_type=AuditRequest.AuditType.DELETED)

    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_visited(audit_storage: StreamAuditStorage, nats):
    context = RequestContext()
    request_context_var.set(context)
    audit_storage.visited("kbid", "uuid", "user", "origin")
    audit_storage.send(context.audit_request)
    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_delete_kb(audit_storage: StreamAuditStorage, nats):
    audit_storage.delete_kb("kbid")

    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))


async def test_search(audit_storage: StreamAuditStorage, nats):
    context = RequestContext()
    request_context_var.set(context)
    audit_storage.search("kbid", "user", 0, "origin", SearchRequest(), -1, 1, "foobar")
    audit_storage.send(context.audit_request)
    await wait_until(partial(stream_audit_finish_condition, audit_storage, 2))


async def test_chat(audit_storage: StreamAuditStorage, nats):
    context = RequestContext()
    request_context_var.set(context)

    audit_storage.chat(
        kbid="kbid",
        user="user",
        client_type=0,
        origin="origin",
        generative_answer_time=1,
        generative_answer_first_chunk_time=1,
        generative_reasoning_first_chunk_time=1,
        rephrase_time=1,
        question="foo",
        rephrased_question="rephrased",
        retrieval_rephrased_question="rephrased_user_query_for_better_retrieval",
        chat_context=[ChatContext(author="USER", text="epa")],
        retrieved_context=[RetrievedContext(text="epa", text_block_id="some/id/path")],
        answer="bar",
        reasoning="reasoning",
        learning_id="learning_id",
        status_code=0,
        model="xx",
    )

    audit_storage.send(context.audit_request)
    await wait_until(partial(stream_audit_finish_condition, audit_storage, 1))

    arg = nats.jetstream().publish.call_args[0][1]
    pb = AuditRequest()
    pb.ParseFromString(arg)

    assert pb.retrieval_rephrased_question == "rephrased_user_query_for_better_retrieval"
    assert pb.chat.question == "foo"
    assert pb.chat.rephrased_question == "rephrased"
    assert pb.chat.answer == "bar"
    assert pb.chat.learning_id == "learning_id"
    assert pb.chat.status_code == 0
    assert pb.chat.model == "xx"
    assert pb.chat.chat_context[0].author == "USER"
    assert pb.chat.chat_context[0].text == "epa"
    assert pb.chat.retrieved_context[0].text_block_id == "some/id/path"
    assert pb.chat.retrieved_context[0].text == "epa"
    assert pb.generative_answer_first_chunk_time == 1
    assert pb.generative_reasoning_first_chunk_time == 1


# ---------------------------------------------------------------------------
# Tests for valid_payload
# ---------------------------------------------------------------------------


class TestValidUserRequest:
    """Tests for the valid_payload helper and the endpoint_validators mapping."""

    @pytest.fixture()
    def make_request(self):
        """Return a factory that creates a minimal mock Request for POST/GET."""

        def _make(method: str, path: str, body: dict | None = None, query: dict | None = None):
            request = MagicMock()
            request.method = method
            request.url.path = path
            request.query_params = query or {}
            request.json = AsyncMock(return_value=body or {})
            return request

        return _make

    # --- chat endpoint ---

    @pytest.mark.asyncio
    async def test_valid_chat_body_returns_json(self, make_request):
        payload = {"question": "hello", "user_id": "u1", "arbitrary_extra": "should_be_gone"}
        request = make_request("POST", "/kb/x/chat", body=payload)
        result = await valid_payload(request, AuditedEndpoint.CHAT)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["question"] == "hello"
        assert parsed["user_id"] == "u1"
        assert "arbitrary_extra" not in parsed  # Should be stripped out
        assert len(parsed) == 2  # Only the two fields should be present

    @pytest.mark.asyncio
    async def test_invalid_chat_body_missing_required_field_returns_none(self, make_request):
        """A body that fails validation (missing required field) should return None."""
        # 'question' and 'user_id' are both required
        payload = {"question": "hello"}  # missing user_id
        request = make_request("POST", "/kb/x/chat", body=payload)
        result = await valid_payload(request, AuditedEndpoint.CHAT)
        assert result is None

    # --- ask endpoint ---

    @pytest.mark.asyncio
    async def test_valid_ask_body_returns_json(self, make_request):
        payload = {"query": "what is nucliadb?"}
        request = make_request("POST", "/kb/x/ask", body=payload)
        result = await valid_payload(request, AuditedEndpoint.ASK)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["query"] == "what is nucliadb?"

    @pytest.mark.asyncio
    async def test_invalid_ask_body_missing_required_field_returns_none(self, make_request):
        # 'query' is required for AskRequest
        payload = {}
        request = make_request("POST", "/kb/x/ask", body=payload)
        result = await valid_payload(request, AuditedEndpoint.ASK)
        assert result is None

    # --- find / search endpoints (no required fields) ---

    @pytest.mark.asyncio
    async def test_valid_find_empty_body_returns_json(self, make_request):
        request = make_request("POST", "/kb/x/find", body={})
        result = await valid_payload(request, AuditedEndpoint.FIND)
        assert result is not None

    @pytest.mark.asyncio
    async def test_valid_search_empty_body_returns_json(self, make_request):
        request = make_request("POST", "/kb/x/search", body={})
        result = await valid_payload(request, AuditedEndpoint.SEARCH)
        assert result is not None

    # --- malformed JSON ---

    @pytest.mark.asyncio
    async def test_invalid_json_returns_none(self, make_request):
        request = make_request("POST", "/kb/x/ask", body=None)
        request.json = AsyncMock(side_effect=json.JSONDecodeError("bad json", "", 0))
        result = await valid_payload(request, AuditedEndpoint.ASK)
        assert result is None

    # --- audit_metadata field pass-through ---

    @pytest.mark.asyncio
    async def test_audit_metadata_is_kept(self, make_request):
        payload = {
            "question": "hello",
            "user_id": "u1",
            "audit_metadata": {"env": "prod", "team": "search"},
        }
        request = make_request("POST", "/kb/x/chat", body=payload)
        result = await valid_payload(request, AuditedEndpoint.CHAT)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["audit_metadata"] == {"env": "prod", "team": "search"}


class TestValidQueryParams:
    """Tests for the valid_query_params helper."""

    @pytest.fixture()
    def make_request(self):
        def _make(path: str, query: dict | None = None):
            request = MagicMock()
            request.url.path = path
            request.query_params = query or {}
            return request

        return _make

    @pytest.mark.asyncio
    async def test_known_fields_are_kept(self, make_request):
        """Only fields that exist on the validator model should be retained."""
        # 'query' is a known field on SearchRequest
        request = make_request("/kb/x/search", query={"query": "hello", "unknown_field": "x"})
        result = await valid_query_params(request, AuditedEndpoint.SEARCH)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["query"] == "hello"
        assert "unknown_field" not in parsed

    @pytest.mark.asyncio
    async def test_all_unknown_fields_returns_empty_dict(self, make_request):
        """If no query params match any model field, an empty JSON object is returned."""
        request = make_request("/kb/x/find", query={"totally_unknown": "val"})
        result = await valid_query_params(request, AuditedEndpoint.FIND)
        assert result is not None
        assert json.loads(result) == {}

    @pytest.mark.asyncio
    async def test_empty_query_params_returns_empty_dict(self, make_request):
        request = make_request("/kb/x/ask", query={})
        result = await valid_query_params(request, AuditedEndpoint.ASK)
        assert result is not None
        assert json.loads(result) == {}

    @pytest.mark.asyncio
    async def test_multiple_known_fields_are_all_kept(self, make_request):
        # 'query' and 'vectorset' are both known fields on SearchRequest
        request = make_request("/kb/x/search", query={"query": "hello", "vectorset": "my-vs"})
        result = await valid_query_params(request, AuditedEndpoint.SEARCH)
        assert result is not None
        parsed = json.loads(result)
        assert parsed["query"] == "hello"
        assert parsed["vectorset"] == "my-vs"
