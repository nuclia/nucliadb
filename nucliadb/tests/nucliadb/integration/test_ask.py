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

from nucliadb.search.predict import AnswerStatusCode, DummyPredictEngine
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    AskRequest,
    AskResponseItem,
    ChatRequest,
    FieldExtensionStrategy,
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
from nucliadb_protos.utils_pb2 import Relation, RelationMetadata, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from tests.utils import inject_message
from tests.utils.dirty_index import wait_for_sync


@pytest.fixture(scope="function", autouse=True)
def audit():
    audit_mock = mock.Mock(chat=mock.AsyncMock())
    with mock.patch("nucliadb.search.search.chat.query.get_audit", return_value=audit_mock):
        yield audit_mock


@pytest.mark.deploy_modes("standalone")
async def test_ask(
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
):
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 200

    context = [{"author": "USER", "text": "query"}]
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "query",
            "context": context,
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
    knowledgebox: str,
    find_incomplete_results,
):
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 529
    assert resp.json() == {"detail": "Temporary error on information retrieval. Please try again."}


@pytest.fixture
async def resource(nucliadb_writer: AsyncClient, knowledgebox: str):
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


@pytest.fixture
async def graph_resource(nucliadb_writer: AsyncClient, nucliadb_ingest_grpc, knowledgebox):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "summary": "Test knowledge graph",
            "texts": {
                "inception1": {"body": "Christopher Nolan directed Inception. Very interesting movie."},
                "inception2": {"body": "Leonardo DiCaprio starred in Inception."},
                "inception3": {"body": "Joseph Gordon-Levitt starred in Inception."},
                "leo": {"body": "Leonardo DiCaprio is a great actor. DiCaprio started acting in 1989."},
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    nodes = {
        "nolan": RelationNode(
            value="Christopher Nolan", ntype=RelationNode.NodeType.ENTITY, subtype="DIRECTOR"
        ),
        "inception": RelationNode(
            value="Inception", ntype=RelationNode.NodeType.ENTITY, subtype="MOVIE"
        ),
        "leo": RelationNode(
            value="Leonardo DiCaprio", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"
        ),
        "dicaprio": RelationNode(value="DiCaprio", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"),
        "levitt": RelationNode(
            value="Joseph Gordon-Levitt", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"
        ),
    }
    edges = [
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["nolan"],
            to=nodes["inception"],
            relation_label="directed",
            metadata=RelationMetadata(
                # Set this field id as int enum value since this is how legacy relations reported paragraph_id
                paragraph_id=rid + "/4/inception1/0-37",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["leo"],
            to=nodes["inception"],
            relation_label="starred",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/inception2/0-39",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["levitt"],
            to=nodes["inception"],
            relation_label="starred",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/inception3/0-42",
                data_augmentation_task_id="",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["leo"],
            to=nodes["dicaprio"],
            relation_label="analogy",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/leo/0-70",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
    ]
    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = knowledgebox
    bm.relations.extend(edges)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()
    return rid


@pytest.mark.deploy_modes("standalone")
async def test_ask_synchronous(nucliadb_reader: AsyncClient, knowledgebox: str, resource):
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_status_code_no_retrieval_data(nucliadb_reader: AsyncClient, knowledgebox: str):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    assert resp_data.answer == "Not enough data to answer this."
    assert len(resp_data.retrieval_results.resources) == 0
    assert resp_data.status == AnswerStatusCode.NO_RETRIEVAL_DATA.prettify()


@pytest.mark.deploy_modes("standalone")
async def test_ask_with_citations(nucliadb_reader: AsyncClient, knowledgebox: str, resource):
    citations = {"foo": [], "bar": []}  # type: ignore
    citations_gen = CitationsGenerativeResponse(citations=citations)
    citations_chunk = GenerativeChunk(chunk=citations_gen)

    predict = get_predict()
    predict.ndjson_answer.append(citations_chunk.model_dump_json() + "\n")  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
    nucliadb_reader: AsyncClient, knowledgebox: str, resource, debug
):
    # Make sure prompt context is returned if debug is True
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={"query": "title", "debug": debug},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    resp_data = SyncAskResponse.model_validate_json(resp.content)
    if debug:
        assert resp_data.prompt_context
        assert resp_data.predict_request
        assert isinstance(resp_data.predict_request, dict)
    else:
        assert resp_data.prompt_context is None
        assert resp_data.predict_request is None


@pytest.fixture
async def resources(nucliadb_writer: AsyncClient, knowledgebox: str):
    kbid = knowledgebox
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
async def test_ask_rag_options_full_resource(nucliadb_reader: AsyncClient, knowledgebox: str, resources):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
    nucliadb_reader: AsyncClient, knowledgebox: str, resources
):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
    nucliadb_reader: AsyncClient, knowledgebox: str, resources
):
    resource1, resource2 = resources

    predict = get_predict()
    predict.calls.clear()  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
async def test_ask_capped_context(nucliadb_reader: AsyncClient, knowledgebox: str, resources):
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
    assert resp.status_code == 200, resp.text
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_on_a_kb_not_found(nucliadb_reader: AsyncClient):
    resp = await nucliadb_reader.post("/kb/unknown_kb_id/ask", json={"query": "title"})
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_ask_max_tokens(nucliadb_reader: AsyncClient, knowledgebox, resources):
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
    assert isinstance(predict, DummyPredictEngine), "dummy is expected in this test"

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        json={
            "query": "title",
            "max_tokens": {"context": predict.max_context + 1},
        },
    )
    assert resp.status_code == 412


@pytest.mark.deploy_modes("standalone")
async def test_ask_on_resource(nucliadb_reader: AsyncClient, knowledgebox: str, resource):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/resource/{resource}/ask",
        json={"query": "title"},
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    SyncAskResponse.model_validate_json(resp.content)


@pytest.mark.deploy_modes("standalone")
async def test_ask_handles_stream_errors_on_predict(
    nucliadb_reader: AsyncClient, knowledgebox, resource
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_handles_stream_unexpected_errors_sync(
    nucliadb_reader: AsyncClient, knowledgebox: str, resource
):
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_handles_stream_unexpected_errors_stream(
    nucliadb_reader: AsyncClient, knowledgebox: str, resource
):
    with mock.patch(
        "nucliadb.search.search.chat.ask.AskResult._stream",
        side_effect=ValueError("foobar"),
    ):
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


@pytest.mark.deploy_modes("standalone")
async def test_ask_with_json_schema_output(
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    resource,
):
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/ask", json={"query": "query"})
    assert resp.status_code == 200

    predict = get_predict()

    predict_answer = JSONGenerativeResponse(object={"answer": "valid answer to", "confidence": 0.5})
    predict.ndjson_answer = [GenerativeChunk(chunk=predict_answer).model_dump_json() + "\n"]  # type: ignore

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
    assert len(results) == 4
    assert results[0].item.type == "answer_json"
    answer_json = results[0].item.object
    assert answer_json["answer"] == "valid answer to"
    assert answer_json["confidence"] == 0.5


@pytest.mark.deploy_modes("standalone")
async def test_ask_assert_audit_retrieval_contexts(
    nucliadb_reader: AsyncClient, knowledgebox: str, resources, audit
):
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/ask", json={"query": "title", "debug": True})
    assert resp.status_code == 200

    retrieved_context = audit.chat.call_args_list[0].kwargs["retrieved_context"]
    assert {(f"{rid}/a/title/0-11", f"The title {i}") for i, rid in enumerate(resources)} == {
        (a.text_block_id, a.text) for a in retrieved_context
    }


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_neighbouring_paragraphs(
    nucliadb_reader: AsyncClient, knowledgebox: str, resources
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
    nucliadb_reader: AsyncClient, knowledgebox: str, resources
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
            f"/kb/{knowledgebox}/ask",
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
async def test_ask_top_k(nucliadb_reader: AsyncClient, knowledgebox: str, resources):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
        f"/kb/{knowledgebox}/ask",
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
@patch("nucliadb.search.search.graph_strategy.get_predict")
@patch("nucliadb.search.search.graph_strategy.rank_relations_reranker")
@patch("nucliadb.search.search.graph_strategy.rank_relations_generative")
@pytest.mark.deploy_modes("standalone")
async def test_ask_graph_strategy(
    mocker_generative,
    mocker_reranker,
    mocker_predict,
    relation_ranking: str,
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    graph_resource,
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
        "debug": True,
    }
    headers = {"X-Synchronous": "True"}

    url = f"/kb/{knowledgebox}/ask"

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
    predict_mock = AsyncMock()
    predict_mock.detect_entities.return_value = [
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
    mocker_predict.return_value = predict_mock

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


@pytest.mark.deploy_modes("standalone")
async def test_ask_rag_strategy_prequeries(nucliadb_reader: AsyncClient, knowledgebox: str, resources):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
    knowledgebox: str,
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
    knowledgebox: str,
    resources,
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
async def test_ask_on_resource_with_json_schema_automatic_prequeries(
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    resource,
):
    kbid = knowledgebox
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
    knowledgebox: str,
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
            f"/kb/{knowledgebox}/ask",
            headers={"X-Synchronous": "True"},
            json={
                "query": "title",
                "rag_strategies": [strategy.dict() for strategy in combination],
            },
        )
        assert resp.status_code == 200, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_ask_fails_with_answer_json_schema_too_big(
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    resources: list[str],
):
    kbid = knowledgebox
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
    knowledgebox: str,
    resources: list[str],
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
        headers={"X-Synchronous": "True"},
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200, resp.text

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
async def test_ask_skip_answer_generation(nucliadb_reader: AsyncClient, knowledgebox: str, resource):
    # Synchronous
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/ask",
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
        f"/kb/{knowledgebox}/ask",
        json={"query": "title", "generate_answer": False, "debug": True},
    )
    assert resp.status_code == 200
    results = parse_ask_response(resp)
    assert results[0].item.type == "retrieval"
    assert len(results[0].item.results.resources) > 0
    assert results[1].item.type == "status"
    assert results[1].item.status == AnswerStatusCode.SUCCESS.prettify()
    assert results[2].item.type == "debug"
    assert results[2].item.metadata["prompt_context"] is not None
    assert results[2].item.metadata["predict_request"] is not None
