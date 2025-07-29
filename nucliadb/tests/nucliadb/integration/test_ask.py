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
from itertools import combinations
from unittest import mock
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from nuclia_models.predict.generative_responses import (
    CitationsGenerativeResponse,
    GenerativeChunk,
    JSONGenerativeResponse,
    StatusGenerativeResponse,
)
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.broker_messages.fields import FieldBuilder
from tests.utils.dirty_index import mark_dirty, wait_for_sync

from nucliadb.search.predict import AnswerStatusCode, DummyPredictEngine
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    AskRequest,
    AskResponseItem,
    AugmentedTextBlock,
    ChatRequest,
    FieldExtensionStrategy,
    FindParagraph,
    FindRequest,
    FullResourceStrategy,
    HierarchyResourceStrategy,
    MetadataExtensionStrategy,
    MetadataExtensionType,
    PreQueriesStrategy,
    PreQuery,
    RagStrategies,
    SyncAskResponse,
)
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_protos import writer_pb2 as wpb2
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.fixture(scope="function", autouse=True)
def audit():
    audit_mock = mock.Mock(chat=mock.AsyncMock())
    with mock.patch("nucliadb.search.search.chat.query.get_audit", return_value=audit_mock):
        yield audit_mock


@pytest.mark.deploy_modes("standalone")
async def test_ask(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    resp = await nucliadb_reader.post(f"/kb/{standalone_knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 200

    chat_history = [{"author": "USER", "text": "query"}]
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "query",
            "chat_history": chat_history,
        },
    )
    assert resp.status_code == 200


@pytest.fixture(scope="function")
def find_incomplete_results():
    with mock.patch(
        "nucliadb.search.search.chat.query.find",
        return_value=(mock.MagicMock(), True, None),
    ):
        yield


@pytest.mark.deploy_modes("standalone")
async def test_ask_handles_incomplete_find_results(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    find_incomplete_results,
):
    resp = await nucliadb_reader.post(f"/kb/{standalone_knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 530
    assert resp.json() == {"detail": "Temporary error on information retrieval. Please try again."}


@pytest.fixture
async def resource(nucliadb_writer: AsyncClient, standalone_knowledgebox: str):
    kbid = standalone_knowledgebox
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

    await mark_dirty()
    await wait_for_sync()

    yield rid


@pytest.mark.deploy_modes("standalone")
async def test_ask_synchronous(nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.answer == "valid answer to"
    assert len(resp_data.retrieval_results.resources) == 1
    assert resp_data.status == AnswerStatusCode.SUCCESS.prettify()


@pytest.mark.deploy_modes("standalone")
async def test_ask_status_code_no_retrieval_data(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str
):
    ask_payload = {
        "query": "title",
        "rag_strategies": [
            {
                "name": "prequeries",
                "queries": [
                    {
                        "request": {"query": "foobar"},
                        "weight": 20,
                    },
                ],
            }
        ],
    }

    # Sync ask
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json=ask_payload,
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.answer == "Not enough data to answer this."
    assert len(resp_data.retrieval_results.resources) == 0
    assert resp_data.prequeries is not None
    assert len(resp_data.prequeries.popitem()[1].resources) == 0
    assert resp_data.status == AnswerStatusCode.NO_RETRIEVAL_DATA.prettify()

    # Stream ask
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json=ask_payload,
    )
    assert resp.status_code == 200
    results = parse_ask_response(resp)
    assert results[0].item.type == "status"
    assert results[0].item.status == AnswerStatusCode.NO_RETRIEVAL_DATA.prettify()
    answer = results[1].item
    assert answer.type == "answer"
    assert answer.text == "Not enough data to answer this."
    main_results = results[2]
    assert main_results.item.type == "retrieval"
    assert len(main_results.item.results.resources) == 0
    prequeries_results = results[3]
    assert prequeries_results.item.type == "prequeries"
    assert len(prequeries_results.item.results) == 1
    assert len(prequeries_results.item.results.popitem()[1].resources) == 0


@pytest.mark.deploy_modes("standalone")
async def test_ask_with_citations(nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource):
    citations = {"foo": [], "bar": []}  # type: ignore
    citations_gen = CitationsGenerativeResponse(citations=citations)
    citations_chunk = GenerativeChunk(chunk=citations_gen)

    predict = get_predict()
    predict.ndjson_answer.append(citations_chunk.model_dump_json() + "\n")  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title", "citations": True, "citation_threshold": 0.5},
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 200

    resp_data = SyncAskResponse.model_validate_json(resp.content)
    resp_citations = resp_data.citations
    assert resp_citations == citations


@pytest.mark.parametrize("debug", (True, False))
@pytest.mark.deploy_modes("standalone")
async def test_sync_ask_returns_debug_mode(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource, debug
):
    # Make sure prompt context is returned if debug is True
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title", "debug": debug},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    if debug:
        assert resp_data.prompt_context
        assert resp_data.predict_request
        assert isinstance(resp_data.predict_request, dict)
        assert isinstance(resp_data.debug, dict)
        assert len(resp_data.debug["metrics"]) > 0
    else:
        assert resp_data.prompt_context is None
        assert resp_data.predict_request is None
        assert resp_data.debug is None


@pytest.fixture
async def resources(nucliadb_writer: AsyncClient, standalone_knowledgebox: str):
    kbid = standalone_knowledgebox
    rids = []
    for i in range(2):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": f"The title {i}",
                "summary": f"The summary {i}",
                "texts": {"text_field": {"body": "The body of the text field"}},
                "origin": {
                    "url": f"https://example.com/{i}",
                    "collaborators": [f"collaborator_{i}"],
                    "metadata": {"foo": "bar"},
                },
                "usermetadata": {"classifications": [{"labelset": "ls", "label": f"rs-{i}"}]},
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_options_full_resource(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources
):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "features": ["keyword", "semantic", "relations"],
            "rag_strategies": [{"name": "full_resource"}],
        },
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_full_resource_rag_strategy_with_exclude(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources
):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "features": ["keyword", "semantic", "relations"],
            "rag_strategies": [
                {
                    "name": "full_resource",
                    "apply_to": {
                        "exclude": ["/classification.labels/ls/rs-0"],
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200
    ask_response = parse_ask_response(resp)
    retrieval = None
    for item in ask_response:
        if item.item.type == "retrieval":
            retrieval = item.item.results
            break
    assert retrieval is not None
    paragraphs_ids = set(
        (
            paragraph.id
            for resource in retrieval.resources.values()
            for field in resource.fields.values()
            for paragraph in field.paragraphs.values()
        )
    )
    assert paragraphs_ids == set((f"{resource1}/a/title/0-11", f"{resource2}/a/title/0-11"))

    # Make sure the prompt context is properly crafted
    assert predict.calls[-2][0] == "chat_query_ndjson"  # type: ignore
    prompt_context = predict.calls[-2][1].query_context  # type: ignore

    # Both titles have matched but resource 1 has been excluded from full
    # resource, so only the matching paragraph gets in the context
    assert len(prompt_context) == 4
    assert prompt_context[f"{resource1}/a/title/0-11"] == "The title 0"
    assert prompt_context[f"{resource2}/a/title"] == "The title 1"
    assert prompt_context[f"{resource2}/a/summary"] == "The summary 1"
    assert prompt_context[f"{resource2}/t/text_field"] == "The body of the text field"


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_options_extend_with_fields(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources
):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "features": ["keyword", "semantic", "relations"],
            "rag_strategies": [{"name": "field_extension", "fields": ["a/summary"]}],
        },
    )
    assert resp.status_code == 200, resp.text
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
            # full_resource can only be combined with metadata extension
            {
                "query": "title",
                "rag_strategies": [
                    {"name": "full_resource"},
                    {"name": "field_extension", "fields": ["a/summary"]},
                ],
            },
            "The following strategies cannot be combined in the same request: field_extension, full_resource",
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
            "must be defined using a valid",
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
@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategies_validation(
    nucliadb_reader: AsyncClient, invalid_payload, expected_error_msg
):
    # Invalid strategy as a string
    resp = await nucliadb_reader.post(
        f"/kb/kbid/ask",
        json=invalid_payload,
    )
    assert resp.status_code == 422
    if expected_error_msg:
        error_msg = resp.json()["detail"][0]["msg"]
        assert expected_error_msg in error_msg


@pytest.mark.deploy_modes("standalone")
async def test_ask_capped_context(nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources):
    # By default, max size is big enough to fit all the prompt context
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "rag_strategies": [{"name": "full_resource"}],
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.prompt_context is not None
    assert len(resp_data.prompt_context) == 6
    total_size = sum(len(v) for v in resp_data.prompt_context)
    # Try now setting a smaller max size. It should be respected
    max_size = 28
    assert total_size > max_size * 3

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_on_a_kb_not_found(nucliadb_reader: AsyncClient):
    resp = await nucliadb_reader.post("/kb/unknown_kb_id/ask", json={"query": "title"})
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_ask_max_tokens(nucliadb_reader: AsyncClient, standalone_knowledgebox, resources):
    # As an integer
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "max_tokens": 100,
        },
    )
    assert resp.status_code == 200

    # Same but with the max tokens in a dict
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "max_tokens": {"context": 100, "answer": 50},
        },
    )
    assert resp.status_code == 200

    # If the context requested is bigger than the max tokens, it should fail
    predict = get_predict()
    assert isinstance(predict, DummyPredictEngine), "dummy is expected in this test"

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "max_tokens": {"context": predict.max_context + 1},
        },
    )
    assert resp.status_code == 412


@pytest.mark.deploy_modes("standalone")
async def test_ask_on_resource(nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/resource/{resource}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    SyncAskResponse.model_validate_json(resp.content)

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/resource/{resource}/ask",
        json={"query": "title", "fields": ["t"]},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    SyncAskResponse.model_validate_json(resp.content)

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/resource/{resource}/ask",
        json={"query": "title", "filter_expression": {"field": {"prop": "field", "type": "text"}}},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    SyncAskResponse.model_validate_json(resp.content)


@pytest.mark.deploy_modes("standalone")
async def test_ask_with_filter_expression(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource
):
    # Search filtering in the field where we know there should be data
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "features": ["keyword"],
            "filter_expression": {"field": {"prop": "field", "type": "generic", "name": "title"}},
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    ask_resp = SyncAskResponse.model_validate_json(resp.content)
    assert len(ask_resp.retrieval_best_matches) > 0

    # Search filtering in the field where we know there should be no data
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "filter_expression": {"field": {"prop": "field", "type": "text", "name": "foobar"}},
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    ask_resp = SyncAskResponse.model_validate_json(resp.content)
    assert len(ask_resp.retrieval_best_matches) == 0


@pytest.mark.deploy_modes("standalone")
async def test_ask_handles_stream_errors_on_predict(
    nucliadb_reader: AsyncClient, standalone_knowledgebox, resource
):
    predict = get_predict()
    assert isinstance(predict, DummyPredictEngine), "dummy is expected in this test"
    prev = predict.ndjson_answer.copy()

    predict.ndjson_answer.pop(-1)
    error_status = StatusGenerativeResponse(code="-1", details="unexpected LLM error")
    status_chunk = GenerativeChunk(chunk=error_status)
    predict.ndjson_answer.append(status_chunk.model_dump_json() + "\n")

    # Sync ask
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    ask_resp = SyncAskResponse.model_validate_json(resp.content)
    assert ask_resp.status == AnswerStatusCode.ERROR.prettify()
    assert ask_resp.error_details == "unexpected LLM error"

    # Stream ask
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title"},
    )
    assert resp.status_code == 200
    results = parse_ask_response(resp)
    status_item = results[-1].item
    assert status_item.type == "status"
    assert status_item.status == AnswerStatusCode.ERROR.prettify()
    assert status_item.details == "unexpected LLM error"

    predict.ndjson_answer = prev


@pytest.mark.deploy_modes("standalone")
async def test_ask_handles_stream_unexpected_errors_sync(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource
):
    with mock.patch(
        "nucliadb.search.search.chat.ask.AskResult._stream",
        side_effect=ValueError("foobar"),
    ):
        # Sync ask -- should return a 500
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/ask",
            json={"query": "title"},
            headers={"X-Synchronous": "True"},
        )
        assert resp.status_code == 500


@pytest.mark.deploy_modes("standalone")
async def test_ask_handles_stream_unexpected_errors_stream(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource
):
    with mock.patch(
        "nucliadb.search.search.chat.ask.AskResult._stream",
        side_effect=ValueError("foobar"),
    ):
        # Stream ask -- should handle by yielding the error item
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/ask",
            json={"query": "title"},
        )
        assert resp.status_code == 200
        results = parse_ask_response(resp)
        error_item = results[-1].item
        assert error_item.type == "error"
        assert (
            error_item.error == "Unexpected error while generating the answer. Please try again later."
        )


@pytest.mark.deploy_modes("standalone")
async def test_ask_with_json_schema_output(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resource,
):
    resp = await nucliadb_reader.post(f"/kb/{standalone_knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 200

    predict = get_predict()

    predict_answer = JSONGenerativeResponse(object={"answer": "valid answer to", "confidence": 0.5})
    predict.ndjson_answer = [GenerativeChunk(chunk=predict_answer).model_dump_json() + "\n"]  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "features": ["keyword", "semantic", "relations"],
            "answer_json_schema": {
                "type": "object",
                "properties": {"answer": {"type": "string"}, "confidence": {"type": "number"}},
            },
        },
    )
    assert resp.status_code == 200, resp.text
    results = parse_ask_response(resp)
    assert len(results) == 5
    assert results[0].item.type == "answer_json"
    answer_json = results[0].item.object
    assert answer_json["answer"] == "valid answer to"
    assert answer_json["confidence"] == 0.5


@pytest.mark.deploy_modes("standalone")
async def test_ask_assert_audit_retrieval_contexts(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources, audit
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask", json={"query": "title", "debug": True}
    )
    assert resp.status_code == 200

    retrieved_context = audit.chat.call_args_list[0].kwargs["retrieved_context"]
    assert {(f"{rid}/a/title/0-11", f"The title {i}") for i, rid in enumerate(resources)} == {
        (a.text_block_id, a.text) for a in retrieved_context
    }


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_neighbouring_paragraphs(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "rag_strategies": [{"name": "neighbouring_paragraphs", "before": 2, "after": 2}],
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.prompt_context is not None


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_metadata_extension(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "rag_strategies": [
                {
                    "name": "metadata_extension",
                    "types": ["origin", "extra_metadata", "classification_labels", "ners"],
                }
            ],
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.prompt_context is not None

    # Make sure the text blocks of the context are extended with the metadata
    origin_found = False
    for text_block in ask_response.prompt_context:
        if "DOCUMENT METADATA AT ORIGIN" in text_block:
            origin_found = True
            assert "https://example.com/" in text_block
            assert "collaborator_" in text_block

    assert origin_found, ask_response.prompt_context

    # Try now combining metadata_extension with another strategy
    for strategy in [
        {"name": "full_resource"},
        {"name": "neighbouring_paragraphs", "before": 1, "after": 1},
        {"name": "hierarchy", "count": 40},
        {"name": "field_extension", "fields": ["a/title", "a/summary"]},
    ]:
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/ask",
            json={
                "query": "title",
                "rag_strategies": [
                    {"name": "metadata_extension", "types": ["origin"]},
                    strategy,
                ],
                "debug": True,
            },
            headers={"X-Synchronous": "True"},
        )
        assert resp.status_code == 200, resp.text
        ask_response = SyncAskResponse.model_validate_json(resp.content)
        assert ask_response.prompt_context is not None

        # Make sure the text blocks of the context are extended with the metadata
        origin_found = False
        for text_block in ask_response.prompt_context:
            if "DOCUMENT METADATA AT ORIGIN" in text_block:
                origin_found = True
                assert "https://example.com/" in text_block
                assert "collaborator_" in text_block
        assert origin_found, ask_response.prompt_context


@pytest.mark.deploy_modes("standalone")
async def test_ask_top_k(nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert len(ask_response.retrieval_results.best_matches) > 1
    prev_best_matches = ask_response.retrieval_results.best_matches

    # Check that the top_k is respected
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "top_k": 1,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert len(ask_response.retrieval_results.best_matches) == 1
    assert ask_response.retrieval_results.best_matches[0] == prev_best_matches[0]


@pytest.mark.parametrize("relation_ranking", ["generative", "reranker"])
@patch("nucliadb.search.search.graph_strategy.rank_relations_reranker")
@patch("nucliadb.search.search.graph_strategy.rank_relations_generative")
@pytest.mark.deploy_modes("standalone")
async def test_ask_graph_strategy(
    mocker_generative,
    mocker_reranker,
    relation_ranking: str,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource,
    dummy_predict,
):
    # Mock the rank_relations functions to return the same relations with a score of 5 (no ranking)
    # This functions are unit tested and require connection to predict
    def mock_rank(relations, *args, **kwargs):
        return relations, {ent: [5 for _ in rels.related_to] for ent, rels in relations.entities.items()}

    mocker_generative.side_effect = mock_rank
    mocker_reranker.side_effect = mock_rank

    data = {
        "query": "Which actors have been in movies directed by Christopher Nolan?",
        "rag_strategies": [
            {
                "name": "graph_beta",
                "hops": 2,
                "top_k": 5,
                "agentic_graph_only": False,
                "query_entity_detection": "suggest",
                "relation_ranking": relation_ranking,
                "relation_text_as_paragraphs": False,
            }
        ],
        "reranker": "noop",
        "debug": True,
    }
    headers = {"X-Synchronous": "True"}

    url = f"/kb/{standalone_knowledgebox}/ask"

    async def assert_ask(d, expected_paragraphs_text, expected_paragraphs_relations):
        resp = await nucliadb_reader.post(
            url,
            json=d,
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        ask_response = SyncAskResponse.model_validate_json(resp.content)
        assert ask_response.status == "success"

        paragraphs = ask_response.prequeries["graph"].resources[graph_resource].fields
        paragraph_texts = {
            p_id: paragraph.text
            for p_id, field in paragraphs.items()
            for paragraph in field.paragraphs.values()
        }
        assert paragraph_texts == expected_paragraphs_text
        paragraph_relations = {
            p_id: [
                {ent, r.relation_label, r.entity}
                for ent, rels in paragraph.relevant_relations.entities.items()
                for r in rels.related_to
            ]
            for p_id, field in paragraphs.items()
            for paragraph in field.paragraphs.values()
            if paragraph.relevant_relations is not None
        }
        assert paragraph_relations == expected_paragraphs_relations

        paragraph_scores = [
            paragraph.score for field in paragraphs.values() for paragraph in field.paragraphs.values()
        ]
        assert all(score == 5 for score in paragraph_scores)

        # We expect a ranking for each hop
        assert mocker_reranker.call_count == 2 or mocker_generative.call_count == 2
        mocker_reranker.reset_mock()
        mocker_generative.reset_mock()

    expected_paragraphs_text = {
        "/t/inception3": "Joseph Gordon-Levitt starred in Inception.",
        "/t/inception2": "Leonardo DiCaprio starred in Inception.",
        "/t/inception1": "Christopher Nolan directed Inception.",
    }
    expected_paragraphs_relations = {
        "/t/inception1": [{"Christopher Nolan", "directed", "Inception"}],
        "/t/inception2": [{"Leonardo DiCaprio", "starred", "Inception"}],
        "/t/inception3": [{"Joseph Gordon-Levitt", "starred", "Inception"}],
    }
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    data["query"] = "In which movie has DiCaprio starred? And Joseph Gordon-Levitt?"
    expected_paragraphs_text = {
        "/t/inception1": "Christopher Nolan directed Inception.",
        "/t/inception3": "Joseph Gordon-Levitt starred in Inception.",
        "/t/inception2": "Leonardo DiCaprio starred in Inception.",
        "/t/leo": "Leonardo DiCaprio is a great actor. DiCaprio started acting in 1989.",
    }
    expected_paragraphs_relations["/t/leo"] = [{"Leonardo DiCaprio", "analogy", "DiCaprio"}]
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    # Setup a mock to test query entity extraction with predict
    dummy_predict.detect_entities = AsyncMock(
        return_value=[
            RelationNode(
                value="DiCaprio",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="ACTOR",
            ),
            RelationNode(
                value="Joseph Gordon-Levitt",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="ACTOR",
            ),
        ]
    )

    # Run the same query but with query_entity_detection set to "predict"
    data["rag_strategies"][0]["query_entity_detection"] = "predict"  # type: ignore

    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    # Now test with relation_text_as_paragraphs
    data["rag_strategies"][0]["relation_text_as_paragraphs"] = True  # type: ignore
    expected_paragraphs_text = {
        "/t/inception2": "- Leonardo DiCaprio starred Inception",
        "/t/leo": "- Leonardo DiCaprio analogy DiCaprio",
        "/t/inception1": "- Christopher Nolan directed Inception",
        "/t/inception3": "- Joseph Gordon-Levitt starred Inception",
    }
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    # Now with agentic graph only
    data["rag_strategies"][0]["agentic_graph_only"] = True  # type: ignore
    data["rag_strategies"][0]["relation_text_as_paragraphs"] = False  # type: ignore
    expected_paragraphs_text = {
        "/t/inception2": "Leonardo DiCaprio starred in Inception.",
        "/t/leo": "Leonardo DiCaprio is a great actor. DiCaprio started acting in 1989.",
    }
    del expected_paragraphs_relations["/t/inception1"]
    del expected_paragraphs_relations["/t/inception3"]
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)


@patch("nucliadb.search.search.graph_strategy.rank_relations_reranker")
@patch("nucliadb.search.search.graph_strategy.rank_relations_generative")
@pytest.mark.deploy_modes("standalone")
async def test_ask_graph_strategy_with_user_relations(
    mocker_generative,
    mocker_reranker,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    dummy_predict,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "DIRECTOR", "value": "Christopher Nolan"},
                        "to": {"type": "entity", "group": "MOVIE", "value": "Inception"},
                        "label": "directed",
                    },
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "ACTOR", "value": "Leonardo DiCaprio"},
                        "to": {"type": "entity", "group": "MOVIE", "value": "Inception"},
                        "label": "starred",
                    },
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "ACTOR", "value": "Joseph Gordon-Levitt"},
                        "to": {"type": "entity", "group": "MOVIE", "value": "Inception"},
                        "label": "starred",
                    },
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "ACTOR", "value": "Leonardo DiCaprio"},
                        "to": {"type": "entity", "group": "ACTOR", "value": "DiCaprio"},
                        "label": "analogy",
                    },
                ]
            },
        },
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Mock the rank_relations functions to return the same relations with a score of 5 (no ranking)
    # This functions are unit tested and require connection to predict
    def mock_rank(relations, *args, **kwargs):
        return relations, {ent: [5 for _ in rels.related_to] for ent, rels in relations.entities.items()}

    mocker_generative.side_effect = mock_rank
    mocker_reranker.side_effect = mock_rank

    # With relation_text_as_paragraphs=False, cannot return user relations (no paragraph)
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "Which actors have been in movies directed by Christopher Nolan?",
            "rag_strategies": [
                {
                    "name": "graph_beta",
                    "hops": 2,
                    "top_k": 5,
                    "agentic_graph_only": False,
                    "query_entity_detection": "suggest",
                    "relation_ranking": "reranker",
                    "relation_text_as_paragraphs": False,
                }
            ],
            "reranker": "noop",
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.status == "no_retrieval_data"

    # With relation_text_as_paragraphs=True, should answer the question
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "Which actors have been in movies directed by Christopher Nolan?",
            "rag_strategies": [
                {
                    "name": "graph_beta",
                    "hops": 2,
                    "top_k": 5,
                    "agentic_graph_only": False,
                    "query_entity_detection": "suggest",
                    "relation_ranking": "reranker",
                    "relation_text_as_paragraphs": True,
                }
            ],
            "reranker": "noop",
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.status == "success"

    fields = ask_response.prequeries["graph"].resources[rid].fields  # type: ignore
    assert list(fields.keys()) == ["/a/usermetadata"]

    paragraph_texts = {
        p_id: paragraph.text for p_id, paragraph in fields["/a/usermetadata"].paragraphs.items()
    }
    assert sorted(list(paragraph_texts.values())) == [
        "- Christopher Nolan directed Inception",
        "- Joseph Gordon-Levitt starred Inception",
        # "- Leonardo DiCaprio analogy DiCaprio", # Only with 3+ hops
        "- Leonardo DiCaprio starred Inception",
    ]


@pytest.mark.deploy_modes("standalone")
async def test_ask_graph_strategy_inner_fuzzy_prefix_search(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource,
    dummy_predict,
):
    from nucliadb.search.search.graph_strategy import fuzzy_search_entities

    kbid = standalone_knowledgebox

    related = await fuzzy_search_entities(
        kbid, "Which actors have been in movies directed by Christopher Nolan?"
    )
    assert related is not None
    assert related.total == 1 == len(related.entities)
    related.entities[0].value == "Christopher Nolan"
    related.entities[0].family == "DIRECTOR"

    related = await fuzzy_search_entities(kbid, "Did Leonard and Joseph perform in the same film?")
    assert related is not None
    assert related.total == 2 == len(related.entities)
    assert set((r.value, r.family) for r in related.entities) == {
        ("Leonardo DiCaprio", "ACTOR"),
        ("Joseph Gordon-Levitt", "ACTOR"),
    }


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_prequeries(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resources
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "",
            "rag_strategies": [
                {
                    "name": "prequeries",
                    "queries": [
                        {
                            "request": {"query": "summary", "fields": ["a/summary"]},
                            "weight": 20,
                        },
                        {
                            "request": {"query": "title", "fields": ["a/title"]},
                            "weight": 1,
                            "id": "title_query",
                        },
                    ],
                }
            ],
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.prequeries is not None
    assert len(ask_response.prequeries) == 2
    print(ask_response.prequeries.keys())
    assert len(ask_response.prequeries["prequery_0"].best_matches) > 1
    assert len(ask_response.prequeries["title_query"].best_matches) > 1


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_prequeries_with_full_resource(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "",
            "rag_strategies": [
                {
                    "name": "full_resource",
                    "count": 2,
                },
                {
                    "name": "prequeries",
                    "queries": [
                        {
                            "request": {"query": "summary", "fields": ["a/summary"]},
                            "weight": 20,
                        },
                        {
                            "request": {"query": "title", "fields": ["a/title"]},
                            "weight": 1,
                            "id": "title_query",
                        },
                    ],
                },
            ],
            "debug": True,
        },
    )
    assert resp.status_code == 200, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_prequeries_with_prefilter(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resources,
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"X-Synchronous": "True"},
        json={
            "query": "",
            "rag_strategies": [
                {
                    "name": "prequeries",
                    "queries": [
                        {
                            "request": {"query": '"The title 0"', "fields": ["a/title"]},
                            "weight": 20,
                            "id": "prefilter_query",
                            "prefilter": True,
                        },
                        {
                            "request": {"query": "summary"},
                            "weight": 1,
                            "id": "prequery",
                        },
                    ],
                },
            ],
            "debug": True,
        },
    )
    expected_rid = resources[0]
    assert resp.status_code == 200, resp.text
    content = resp.json()
    ask_response = SyncAskResponse.model_validate(content)
    assert ask_response.prequeries is not None
    assert len(ask_response.prequeries) == 2

    # Check that the prefilter query found the right resource
    assert len(ask_response.prequeries["prefilter_query"].resources) == 1
    assert expected_rid in ask_response.prequeries["prefilter_query"].resources

    # Check that the other prequery was executed and only matched one resource (due to the prefilter)
    assert len(ask_response.prequeries["prequery"].resources) == 1
    assert ask_response.prequeries["prequery"].resources[expected_rid].title == "The title 0"


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_prequeries_with_prefilter_and_expression(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resources,
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"X-Synchronous": "True"},
        json={
            "query": "",
            "rag_strategies": [
                {
                    "name": "prequeries",
                    "queries": [
                        {
                            "request": {"query": '"The title 0"', "fields": ["a/title"]},
                            "weight": 20,
                            "id": "prefilter_query",
                            "prefilter": True,
                        },
                        {
                            "request": {
                                "query": "summary",
                                "filter_expression": {
                                    "field": {"not": {"prop": "label", "labelset": "empty"}}
                                },
                            },
                            "weight": 1,
                            "id": "prequery",
                        },
                    ],
                },
            ],
            "debug": True,
        },
    )
    expected_rid = resources[0]
    assert resp.status_code == 200, resp.text
    content = resp.json()
    ask_response = SyncAskResponse.model_validate(content)
    assert ask_response.prequeries is not None
    assert len(ask_response.prequeries) == 2

    # Check that the prefilter query found the right resource
    assert len(ask_response.prequeries["prefilter_query"].resources) == 1
    assert expected_rid in ask_response.prequeries["prefilter_query"].resources

    # Check that the other prequery was executed and only matched one resource (due to the prefilter)
    assert len(ask_response.prequeries["prequery"].resources) == 1
    assert ask_response.prequeries["prequery"].resources[expected_rid].title == "The title 0"


@pytest.mark.deploy_modes("standalone")
async def test_ask_on_resource_with_json_schema_automatic_prequeries(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resource,
):
    kbid = standalone_knowledgebox
    rid = resource
    answer_json_schema = {
        "name": "book_ordering",
        "description": "Structured answer for a book to order",
        "parameters": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "The title of the book"},
                "author": {"type": "string", "description": "The author of the book"},
                "ref_num": {"type": "string", "description": "The ISBN of the book"},
                "price": {"type": "number", "description": "The price of the book"},
            },
            "required": ["title", "author", "ref_num", "price"],
        },
    }
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/resource/{rid}/ask",
        headers={"X-Synchronous": "True"},
        json={
            "query": "",
            "features": ["keyword", "semantic"],
            "answer_json_schema": answer_json_schema,
        },
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.prequeries is not None
    assert len(ask_response.prequeries) == 4


@pytest.mark.deploy_modes("standalone")
async def test_all_rag_strategies_combinations(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resources,
):
    rag_strategies = [
        FullResourceStrategy(),
        FieldExtensionStrategy(fields=["a/summary"]),
        MetadataExtensionStrategy(types=list(MetadataExtensionType)),
        HierarchyResourceStrategy(),
        PreQueriesStrategy(queries=[PreQuery(request=FindRequest())]),
    ]

    def valid_combination(combination: list[RagStrategies]) -> bool:
        try:
            ChatRequest(query="foo", rag_strategies=combination)
            return True
        except ValueError:
            return False

    # Create all possible combinations of the list
    valid_combinations = []
    for i in range(1, len(rag_strategies) + 1):
        for combination in combinations(rag_strategies, i):
            if valid_combination(list(combination)):  # type: ignore
                valid_combinations.append(list(combination))

    assert len(valid_combinations) == 19
    for combination in valid_combinations:  # type: ignore
        print(f"Combination: {sorted([strategy.name for strategy in combination])}")
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/ask",
            headers={"X-Synchronous": "True"},
            json={
                "query": "title",
                "rag_strategies": [strategy.model_dump() for strategy in combination],
            },
        )
        assert resp.status_code == 200, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_ask_fails_with_answer_json_schema_too_big(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resources: list[str],
):
    kbid = standalone_knowledgebox
    rid = resources[0]

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/resource/{rid}/ask",
        json=AskRequest(
            query="",
            answer_json_schema={
                "name": "structred_response",
                "description": "Structured response with custom fields",
                "parameters": {
                    "type": "object",
                    "properties": {
                        f"property-{i}": {
                            "type": "string",
                            "description": f"Yet another property... ({i})",
                        }
                        for i in range(50)
                    },
                    "required": ["property-0"],
                },
            },
        ).model_dump(),
    )

    assert resp.status_code == 400
    assert (
        resp.json()["detail"]
        == "Answer JSON schema with too many properties generated too many prequeries"
    )


@pytest.mark.deploy_modes("standalone")
async def test_rag_image_rag_strategies(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resources: list[str],
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"X-Synchronous": "True"},
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200, resp.text

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"X-Synchronous": "True"},
        json={
            "query": "title",
            "rag_images_strategies": [
                {
                    "name": "page_image",
                    "count": 2,
                },
                {
                    "name": "tables",
                },
                {
                    "name": "paragraph_image",
                },
            ],
        },
    )
    assert resp.status_code == 200, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_ask_skip_answer_generation(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource
):
    # Synchronous
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title", "generate_answer": False, "debug": True},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.answer == ""
    assert len(resp_data.retrieval_results.resources) == 1
    assert resp_data.status == AnswerStatusCode.SUCCESS.prettify()
    assert resp_data.prompt_context is not None
    assert resp_data.predict_request is not None

    # Streaming
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title", "generate_answer": False, "debug": True},
    )
    assert resp.status_code == 200
    results = parse_ask_response(resp)
    assert results[0].item.type == "retrieval"
    assert len(results[0].item.results.resources) > 0
    assert results[1].item.type == "status"
    assert results[1].item.status == AnswerStatusCode.SUCCESS.prettify()
    assert results[2].item.type == "augmented_context"
    assert results[3].item.type == "debug"
    assert results[3].item.metadata["prompt_context"] is not None
    assert results[3].item.metadata["predict_request"] is not None


@pytest.mark.deploy_modes("standalone")
async def test_ask_calls_predict_query_once(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource
):
    predict = get_predict()
    assert isinstance(predict, DummyPredictEngine), "dummy is expected in this test"
    assert len(predict.calls) == 0

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={"query": "title", "reranker": "noop"},
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 200

    assert len(predict.calls) == 2
    assert predict.calls[0][0] == "query"
    assert predict.calls[1][0] == "chat_query_ndjson"


@pytest.mark.deploy_modes("standalone")
async def test_ask_chat_history_relevance_threshold(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resources: list[str],
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        headers={"X-Synchronous": "True"},
        json=AskRequest(
            query="title",
            chat_history_relevance_threshold=0.5,
        ).model_dump(),
    )
    assert resp.status_code == 200, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_ask_neighbouring_paragraphs_rag_strategy(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox

    # Create a resource with a text field that has 3 paragraphs
    paragraphs = [
        "Mario is my older brother.",
        "Nuria used be friends with Mario.",
        "We all know each other now.",
    ]
    extracted_text = "\n".join(paragraphs)

    positions = {}
    start = 0
    for i, paragraph in enumerate(paragraphs):
        end = start + len(paragraph)
        positions[i] = (start, end)
        start = end + 1

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={"title": "My resource", "texts": {"text1": {"body": extracted_text}}},
    )
    resp.raise_for_status()
    rid = resp.json()["uuid"]

    # Inject the processing broker message
    bmb = BrokerMessageBuilder(kbid=kbid, rid=rid, source=wpb2.BrokerMessage.MessageSource.PROCESSOR)
    fb = FieldBuilder(
        field="text1",
        field_type=wpb2.FieldType.TEXT,
    )
    fb.with_extracted_text(extracted_text)

    for i, (start, end) in positions.items():
        pbpar = rpb2.Paragraph(
            start=start,
            end=end,
            text=paragraphs[i],
        )
        fb.with_extracted_paragraph_metadata(pbpar)
    bmb.add_field_builder(fb)

    await inject_message(nucliadb_ingest_grpc, bmb.build(), wait_for_ready=True)

    await mark_dirty()
    await wait_for_sync()

    # Now check that neighbouring paragraphs rag strategy works
    # First off, fetch only one of the paragraphs
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        json={
            "query": "Nuria",  # To match the middle paragraph
            "features": ["keyword"],
            "top_k": 1,
            "rag_strategies": [
                {
                    "name": "neighbouring_paragraphs",
                    "before": 0,
                    "after": 0,
                }
            ],
        },
        headers={"x-synchronous": "true"},
    )
    resp.raise_for_status()
    ask = SyncAskResponse.model_validate(resp.json())

    retrieved, augmented = _fetch_paragraphs(ask)
    assert len(retrieved) == 1
    assert retrieved[0].text == paragraphs[1]
    assert len(augmented) == 0

    # Now fetch all neighbouring paragraphs
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        json={
            "query": "Nuria",  # To match the middle paragraph
            "features": ["keyword"],
            "top_k": 1,
            "rag_strategies": [
                {
                    "name": "neighbouring_paragraphs",
                    "before": 1,
                    "after": 1,
                }
            ],
        },
        headers={"x-synchronous": "true"},
    )
    resp.raise_for_status()
    ask = SyncAskResponse.model_validate(resp.json())

    retrieved, augmented = _fetch_paragraphs(ask)
    assert len(retrieved) == 1
    assert retrieved[0].text == paragraphs[1]
    assert len(augmented) == 2
    augmented.sort(key=lambda p: p.id)
    assert augmented[0].text == paragraphs[0]
    assert augmented[0].position
    assert augmented[0].position.start == positions[0][0]
    assert augmented[0].position.end == positions[0][1]
    assert augmented[0].position.index == 0
    assert augmented[1].text == paragraphs[2]
    assert augmented[1].position
    assert augmented[1].position.start == positions[2][0]
    assert augmented[1].position.end == positions[2][1]
    assert augmented[1].position.index == 2

    # Check that combined with hierarchy rag strategy works well
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        json={
            "query": "Nuria",  # To match the middle paragraph
            "features": ["keyword"],
            "top_k": 1,
            "debug": True,
            "rag_strategies": [
                {
                    "name": "hierarchy",
                },
                {
                    "name": "neighbouring_paragraphs",
                    "before": 1,
                    "after": 1,
                },
            ],
        },
        headers={"x-synchronous": "true"},
    )
    resp.raise_for_status()
    ask = SyncAskResponse.model_validate(resp.json())
    prompt_context = ask.prompt_context
    assert prompt_context is not None
    assert len(prompt_context) == 3
    # The retrieved paragraph is the one with the hierarchy information
    assert (
        prompt_context[0]
        == "DOCUMENT: My resource \n SUMMARY:  \n RESOURCE CONTENT: \n EXTRACTED BLOCK: \n Nuria used be friends with Mario."
    )
    assert prompt_context[1] == "Mario is my older brother."
    assert prompt_context[2] == "We all know each other now."
    _, augmented = _fetch_paragraphs(ask)
    assert len(augmented) == 3
    assert augmented[0].augmentation_type.value == "hierarchy"


def _fetch_paragraphs(
    ask_response: SyncAskResponse,
) -> tuple[list[FindParagraph], list[AugmentedTextBlock]]:
    retrieval = [
        paragraph
        for resource in ask_response.retrieval_results.resources.values()
        for field in resource.fields.values()
        for paragraph in field.paragraphs.values()
    ]
    if ask_response.augmented_context is None:
        return retrieval, []
    augmented = [text_block for text_block in ask_response.augmented_context.paragraphs.values()]
    return retrieval, augmented


@pytest.mark.deploy_modes("standalone")
async def test_ask_query_image(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "title",
            "debug": True,
            "query_image": {
                "content_type": "image/png",
                "b64encoded": "dummy_base64_image",
            },
        },
    )
    assert resp.status_code == 200
    predict = get_predict()
    assert isinstance(predict, DummyPredictEngine), "dummy is expected in this test"
    assert len(predict.calls) == 1
    assert predict.calls[0][0] == "query"
    assert predict.calls[0][1]["query_image"].content_type == "image/png"
    assert predict.calls[0][1]["query_image"].b64encoded == "dummy_base64_image"
    assert predict.calls[0][1]["sentence"] == "title"
