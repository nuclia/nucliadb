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
import asyncio
import math
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import nats
import pytest
from httpx import AsyncClient
from nats.aio.client import Client
from nats.js import JetStreamContext
from nucliadb_protos.audit_pb2 import AuditRequest, ClientType
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.ingest.tests.vectors import V1
from nucliadb.search.predict import PredictVectorMissing, SendToPredictError
from nucliadb.search.search.query import pre_process_query
from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.utilities import Utility, clean_utility, get_audit, set_utility


@pytest.mark.asyncio
async def test_simple_search_sc_2062(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    # PUBLIC API
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}")
    assert resp.status_code == 200
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=title")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=summary")
    assert resp.status_code == 200
    assert len(resp.json()["paragraphs"]["results"]) == 1


def broker_resource_with_duplicates(knowledgebox, sentence):
    bm = broker_resource(kbid=knowledgebox)
    paragraph = sentence
    text = f"{paragraph}{paragraph}"
    etw = rpb.ExtractedTextWrapper()
    etw.body.text = text
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of document"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    p1 = rpb.Paragraph(
        start=0,
        end=len(paragraph),
    )
    p1.start_seconds.append(0)
    p1.end_seconds.append(10)

    p2 = rpb.Paragraph(
        start=len(paragraph),
        end=len(paragraph) * 2,
    )
    p2.start_seconds.append(10)
    p2.end_seconds.append(20)
    p2.start_seconds.append(20)
    p2.end_seconds.append(30)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)

    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())

    bm.field_metadata.append(fcm)
    return bm


async def create_resource_with_duplicates(
    knowledgebox, writer: WriterStub, sentence: str
):
    bm = broker_resource_with_duplicates(knowledgebox, sentence=sentence)
    await inject_message(writer, bm)
    return bm.uuid


@pytest.mark.asyncio
async def test_search_filters_out_duplicate_paragraphs(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    await create_resource_with_duplicates(
        knowledgebox, nucliadb_grpc, sentence="My own text Ramon. "
    )
    await create_resource_with_duplicates(
        knowledgebox, nucliadb_grpc, sentence="Another different paragraph with text"
    )

    query = "text"
    # It should filter out duplicates by default
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query={query}")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["paragraphs"]["results"]) == 2

    # It should filter out duplicates if specified
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search?query={query}&with_duplicates=false"
    )
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["paragraphs"]["results"]) == 2

    # It should return duplicates if specified
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search?query={query}&with_duplicates=true"
    )
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["paragraphs"]["results"]) == 4


@pytest.mark.asyncio
async def test_search_returns_paragraph_positions(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    sentence = "My own text Ramon."
    await create_resource_with_duplicates(
        knowledgebox, nucliadb_grpc, sentence=sentence
    )
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=Ramon")
    assert resp.status_code == 200
    content = resp.json()
    position = content["paragraphs"]["results"][0]["position"]
    assert position["start"] == 0
    assert position["end"] == len(sentence)
    assert position["index"] == 0
    assert position["page_number"] is not None


def broker_resource_with_classifications(knowledgebox):
    bm = broker_resource(kbid=knowledgebox)

    text = "Some text"
    etw = rpb.ExtractedTextWrapper()
    etw.body.text = text
    field_id = rpb.FieldID(field="file", field_type=rpb.FieldType.FILE)
    etw.field.CopyFrom(field_id)
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of document"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.CopyFrom(field_id)

    c1 = rpb.Classification()
    c1.label = "label1"
    c1.labelset = "labelset1"
    fcm.metadata.metadata.classifications.append(c1)
    bm.field_metadata.append(fcm)

    c2 = rpb.Classification()
    c2.label = "label2"
    c2.labelset = "labelset1"

    bm.basic.usermetadata.classifications.append(c2)

    p1 = rpb.Paragraph(
        start=0,
        end=len(text),
    )
    p1.start_seconds.append(0)
    p1.end_seconds.append(10)
    p1.classifications.append(c1)

    fcm.metadata.metadata.paragraphs.append(p1)

    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())

    bm.field_metadata.append(fcm)

    return bm


@pytest.mark.asyncio
async def test_search_returns_labels(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    bm = broker_resource_with_classifications(knowledgebox)
    await inject_message(nucliadb_grpc, bm)

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search?query=Some&show=extracted&extracted=metadata",
    )
    assert resp.status_code == 200
    content = resp.json()
    assert content["paragraphs"]["results"]
    par = content["paragraphs"]["results"][0]
    assert par["labels"] == ["labelset1/label2", "labelset1/label1"]

    extracted_metadata = content["resources"][bm.uuid]["data"]["files"]["file"][
        "extracted"
    ]["metadata"]["metadata"]
    assert extracted_metadata["classifications"] == [
        {"label": "label1", "labelset": "labelset1"}
    ]


@pytest.mark.asyncio
async def test_search_with_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    # Inject a resource with a pdf icon
    bm = broker_resource(knowledgebox)
    bm.basic.icon = "application/pdf"

    await inject_message(nucliadb_grpc, bm)

    # Check that filtering by pdf icon returns it
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search?show=basic&filters=/n/i/application/pdf"
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1

    # With a different icon should return no results
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search?show=basic&filters=/n/i/application/docx"
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 0


@pytest.mark.asyncio
async def test_paragraph_search_with_filters(
    nucliadb_writer,
    nucliadb_reader,
    nucliadb_grpc,
    knowledgebox,
):
    kbid = knowledgebox
    # Create a resource with two fields (title and summary)
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Mushrooms",
            "summary": "A very good book about mushrooms (aka funghi)",
        },
        headers={"X-SYNCHRONOUS": "True"},
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/search?query=mushrooms"
    )
    assert resp.status_code == 200
    body = resp.json()
    paragraph_results = body["paragraphs"]["results"]
    assert len(paragraph_results) == 2

    # Check that you can filter results by field
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/search",
        params={"query": "mushrooms", "fields": ["a/summary"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    paragraph_results = body["paragraphs"]["results"]
    assert len(paragraph_results) == 1
    assert paragraph_results[0]["field"] == "summary"


@pytest.mark.asyncio
async def test_catalog_can_filter_by_processing_status(
    nucliadb_reader: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    """
    Test description:
    - Creates a resource for each processing status value.
    - Checks that if not specified, search returns all resources.
    - Checks that search is able to filter by each value.
    - Checks that we can get counts for each processing status
    """
    valid_status = ["PROCESSED", "PENDING", "ERROR"]

    created = 0
    for status_name, status_value in rpb.Metadata.Status.items():
        if status_name not in valid_status:
            continue
        bm = broker_resource(knowledgebox)
        bm.basic.metadata.status = status_value
        await inject_message(nucliadb_grpc, bm)
        created += 1

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == created

    # Two should be PROCESSED (the ERROR is counted as processed)
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "with_status": "PROCESSED",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 2

    # One should be PENDING
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "with_status": "PENDING",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1

    # Get the list of PENDING
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "filters": ["/n/s/PENDING"],
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1

    # Check facets by processing status
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "faceted": ["/n/s"],
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    facets = resp_json["fulltext"]["facets"]
    for status in valid_status:
        assert facets["/n/s"][f"/n/s/{status}"] == 1


@pytest.mark.asyncio
async def test_search_returns_sentence_positions(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    await inject_resource_with_a_sentence(knowledgebox, nucliadb_grpc)
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search", json=dict(query="my own text", min_score=-1)
    )
    assert resp.status_code == 200
    content = resp.json()
    position = content["sentences"]["results"][0]["position"]
    assert position["start"] is not None
    assert position["end"] is not None
    assert position["index"] is not None
    assert "page_number" not in position


async def inject_resource_with_a_sentence(knowledgebox, writer):
    bm = broker_resource(knowledgebox)

    bm.files["file"].file.uri = "http://nofile"
    bm.files["file"].file.size = 0
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer?"
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    bm.extracted_text.append(etw)

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    p1 = rpb.Paragraph(
        start=0,
        end=45,
    )
    p1.start_seconds.append(0)
    p1.end_seconds.append(10)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    bm.field_metadata.append(fcm)

    ev = rpb.ExtractedVectorsWrapper()
    ev.field.field = "file"
    ev.field.field_type = rpb.FieldType.FILE

    v1 = rpb.Vector()
    v1.start = 0
    v1.end = 19
    v1.start_paragraph = 0
    v1.end_paragraph = 45
    v1.vector.extend(V1)
    ev.vectors.vectors.vectors.append(v1)

    bm.field_vectors.append(ev)

    await inject_message(writer, bm)


@pytest.mark.asyncio
async def test_search_relations(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
    knowledge_graph,
):
    relation_nodes, relation_edges = knowledge_graph

    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)

    predict_mock.detect_entities = AsyncMock(
        return_value=[relation_nodes["Becquer"], relation_nodes["Newton"]]
    )

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search",
        params={
            "features": "relations",
            "query": "What relates Newton and Becquer?",
        },
    )
    assert resp.status_code == 200

    entities = resp.json()["relations"]["entities"]
    expected = {
        "Becquer": {
            "related_to": [
                {
                    "entity": "Poetry",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "write",
                    "direction": "out",
                },
                {
                    "entity": "Poetry",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "like",
                    "direction": "out",
                },
                {
                    "entity": "Joan Antoni",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "read",
                    "direction": "in",
                },
            ]
        },
        "Newton": {
            "related_to": [
                {
                    "entity": "Gravity",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "formulate",
                    "direction": "out",
                },
                {
                    "entity": "Physics",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "study",
                    "direction": "out",
                },
            ]
        },
    }

    for entity in expected:
        assert entity in entities
        assert len(entities[entity]["related_to"]) == len(
            expected[entity]["related_to"]
        )

        for expected_relation in expected[entity]["related_to"]:
            assert expected_relation in entities[entity]["related_to"]

    predict_mock.detect_entities = AsyncMock(return_value=[relation_nodes["Animal"]])

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search",
        params={
            "features": "relations",
            "query": "Do you like animals?",
        },
    )
    assert resp.status_code == 200

    entities = resp.json()["relations"]["entities"]
    expected = {
        "Animal": {
            "related_to": [
                {
                    "entity": "Cat",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "species",
                    "direction": "in",
                },
                {
                    "entity": "Swallow",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "species",
                    "direction": "in",
                },
            ]
        },
    }

    for entity in expected:
        assert entity in entities
        assert len(entities[entity]["related_to"]) == len(
            expected[entity]["related_to"]
        )

        for expected_relation in expected[entity]["related_to"]:
            assert expected_relation in entities[entity]["related_to"]


@pytest.mark.asyncio
async def test_processing_status_doesnt_change_on_search_after_processed(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    # Inject a resource with status=PROCESSED
    bm = broker_resource(knowledgebox)
    bm.basic.metadata.status = rpb.Metadata.Status.PROCESSED
    await inject_message(nucliadb_grpc, bm)

    # Check that search for resource list shows it
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "with_status": "PROCESSED",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1

    # Edit the resource so that status=PENDING
    assert (
        await nucliadb_writer.patch(
            f"/kb/{knowledgebox}/resource/{bm.uuid}",
            json={
                "title": "My new title",
            },
            headers={"X-SYNCHRONOUS": "True"},
            timeout=None,
        )
    ).status_code == 200

    # Wait a bit until for the node to index it
    await asyncio.sleep(1)

    # Check that search for resource list still shows it
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "with_status": "PROCESSED",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1

    # Check that facets count it as PENDING though
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "faceted": ["/n/s"],
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    facets = resp_json["fulltext"]["facets"]
    assert facets["/n/s"] == {"/n/s/PENDING": 1}


@pytest.mark.asyncio
async def test_search_pre_processes_query(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json=dict(title="The Good, the Bad and the Ugly.mov"),
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    for query in ("G.o:o!d?", "B;a,d", "U¿g¡ly"):
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/search",
            json={
                "fields": ["a/title"],
                "query": query,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["resources"][rid]

    # Check that if processing the query outputs an empty string we
    # don't return all results
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "fields": ["a/title"],
            "query": "?¿!,",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    # Check that query is not pre-processed if user is searching with double-quotes
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "fields": ["a/title"],
            "query": '"B;a,d"',
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0


@pytest.mark.parametrize(
    "user_query,processed_query",
    [
        ("¿Where is my beer?", "Where is my beer"),  # removes question marks
        ("   My document ", "My document"),  # removes spaces
        ("?¿!;,.:", ""),  # return user query if processed_query == ""
        ("Hola?!", "Hola"),
        (" Hola que tal ? ! asd      ", "Hola que tal asd"),
        (' Hola que "tal ? ! asd     " ', 'Hola que "tal ? ! asd "'),
        ("", ""),
    ],
)
def test_pre_process_query(user_query, processed_query):
    assert pre_process_query(user_query) == processed_query


@pytest.mark.asyncio
async def test_search_automatic_relations(
    nucliadb_reader: AsyncClient, nucliadb_writer: AsyncClient, knowledgebox
):
    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"X-Synchronous": "true"},
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
            "origin": {
                "colaborators": ["Anne", "John"],
            },
            "usermetadata": {
                "classifications": [
                    {"labelset": "animals", "label": "cat"},
                    {"labelset": "food", "label": "cookie"},
                ],
                "relations": [
                    {
                        "relation": "CHILD",
                        "to": {
                            "type": "resource",
                            "value": "sub-document",
                        },
                    },
                    {
                        "relation": "ABOUT",
                        "to": {
                            "type": "label",
                            "value": "label",
                        },
                    },
                    {
                        "relation": "ENTITY",
                        "to": {"type": "entity", "value": "cat", "group": "ANIMAL"},
                    },
                    {
                        "relation": "COLAB",
                        "to": {
                            "type": "user",
                            "value": "Pepita",
                            "group": "PERSON",
                        },
                    },
                    {
                        "relation": "OTHER",
                        "to": {
                            "type": "entity",
                            "value": "other",
                            "group": "things",
                        },
                    },
                ],
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Search resource relations
    rn = RelationNode(
        value=rid,
        ntype=RelationNode.NodeType.RESOURCE,
    )
    predict_mock.detect_entities = AsyncMock(return_value=[rn])

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search",
        params={
            "features": "relations",
            "query": "Relations for this resource",
        },
    )
    assert resp.status_code == 200

    entities = resp.json()["relations"]["entities"]
    expected = {
        rid: {
            "related_to": [
                {
                    "entity": "Pepita",
                    "entity_type": "user",
                    "relation": "COLAB",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "Anne",
                    "entity_type": "user",
                    "relation": "COLAB",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "John",
                    "entity_type": "user",
                    "relation": "COLAB",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "cat",
                    "entity_type": "entity",
                    "relation": "ENTITY",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "label",
                    "entity_type": "label",
                    "relation": "ABOUT",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "animals/cat",
                    "entity_type": "label",
                    "relation": "ABOUT",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "food/cookie",
                    "entity_type": "label",
                    "relation": "ABOUT",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "sub-document",
                    "entity_type": "resource",
                    "relation": "CHILD",
                    "relation_label": "",
                    "direction": "out",
                },
                {
                    "entity": "other",
                    "entity_type": "entity",
                    "relation": "OTHER",
                    "relation_label": "",
                    "direction": "out",
                },
            ]
        }
    }

    for entity in expected:
        assert entity in entities
        assert len(entities[entity]["related_to"]) == len(
            expected[entity]["related_to"]
        )

        assert sorted(
            expected[entity]["related_to"], key=lambda x: x["entity"]
        ) == sorted(entities[entity]["related_to"], key=lambda x: x["entity"])

    # Search a colaborator
    rn = RelationNode(
        value="John",
        ntype=RelationNode.NodeType.USER,
    )
    predict_mock.detect_entities = AsyncMock(return_value=[rn])

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search",
        params={
            "features": "relations",
            "query": "You know John?",
        },
    )
    assert resp.status_code == 200

    entities = resp.json()["relations"]["entities"]
    expected = {
        "John": {
            "related_to": [
                {
                    "entity": rid,
                    "entity_type": "resource",
                    "relation": "COLAB",
                    "relation_label": "",
                    "direction": "in",
                }
            ]
        }
    }

    for entity in expected:
        assert entity in entities
        assert len(entities[entity]["related_to"]) == len(
            expected[entity]["related_to"]
        )

        assert sorted(
            expected[entity]["related_to"], key=lambda x: x["entity"]
        ) == sorted(entities[entity]["related_to"], key=lambda x: x["entity"])


@pytest.mark.asyncio
async def test_only_search_and_suggest_calls_audit(nucliadb_reader, knowledgebox):
    kbid = knowledgebox

    audit = get_audit()
    audit.search = AsyncMock()
    audit.suggest = AsyncMock()

    resp = await nucliadb_reader.get(f"/kb/{kbid}/catalog", params={"query": ""})
    assert resp.status_code == 200

    audit.search.assert_not_awaited()

    resp = await nucliadb_reader.get(f"/kb/{kbid}/search")
    assert resp.status_code == 200

    audit.search.assert_awaited_once()

    resp = await nucliadb_reader.get(f"/kb/{kbid}/suggest?query=")

    assert resp.status_code == 200

    audit.suggest.assert_awaited_once()


async def get_audit_messages(sub):
    msg = await sub.fetch(1)
    auditreq = AuditRequest()
    auditreq.ParseFromString(msg[0].data)
    return auditreq


@pytest.mark.asyncio
async def test_search_and_suggest_sent_audit(
    nucliadb_reader,
    knowledgebox,
    stream_audit: StreamAuditStorage,
):
    from nucliadb_utils.settings import audit_settings

    kbid = knowledgebox

    # Prepare a test audit stream to receive our messages
    partition = stream_audit.get_partition(kbid)
    client: Client = await nats.connect(stream_audit.nats_servers)
    jetstream: JetStreamContext = client.jetstream()
    if audit_settings.audit_jetstream_target is None:
        assert False, "Missing jetstream target in audit settings"
    subject = audit_settings.audit_jetstream_target.format(
        partition=partition, type="*"
    )

    set_utility(Utility.AUDIT, stream_audit)
    try:
        await jetstream.delete_stream(name=audit_settings.audit_stream)
    except nats.js.errors.NotFoundError:
        pass

    await jetstream.add_stream(name=audit_settings.audit_stream, subjects=[subject])
    psub = await jetstream.pull_subscribe(subject, "psub")

    # Test search sends audit
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search", headers={"x-ndb-client": "chrome_extension"}
    )
    assert resp.status_code == 200

    auditreq = await get_audit_messages(psub)

    assert auditreq.kbid == kbid
    assert auditreq.type == AuditRequest.AuditType.SEARCH
    assert (
        auditreq.client_type == ClientType.CHROME_EXTENSION
    )  # Just to use other that the enum default
    try:
        int(auditreq.trace_id)
    except ValueError:
        assert False, "Invalid trace ID"

    # Test suggest sends audit
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/suggest?query=", headers={"x-ndb-client": "chrome_extension"}
    )
    assert resp.status_code == 200

    auditreq = await get_audit_messages(psub)

    assert auditreq.kbid == kbid
    assert auditreq.type == AuditRequest.AuditType.SUGGEST
    assert (
        auditreq.client_type == ClientType.CHROME_EXTENSION
    )  # Just to use other that the enum default

    try:
        int(auditreq.trace_id)
    except ValueError:
        assert False, "Invalid trace ID"

    clean_utility(Utility.AUDIT)


@pytest.mark.asyncio
async def test_search_pagination(
    nucliadb_reader: AsyncClient,
    ten_quick_dummy_resources_kb,
):
    kbid = ten_quick_dummy_resources_kb

    total = 20  # 10 titles and 10 summaries
    page_size = 5

    for feature, result_key in [
        ("paragraph", "paragraphs"),
        ("document", "fulltext"),
    ]:
        total_pages = math.floor(total / page_size)
        for page_number in range(0, total_pages):
            resp = await nucliadb_reader.get(
                f"/kb/{kbid}/search",
                params={
                    "features": [feature],
                    "page_number": page_number,
                    "page_size": page_size,
                },
            )
            assert resp.status_code == 200
            body = resp.json()[result_key]
            assert body["next_page"] == (page_number != total_pages - 1)
            assert len(body["results"]) == body["page_size"] == page_size

        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/search",
            params={
                "features": [feature],
                "page_number": page_number + 1,
                "page_size": page_size,
            },
        )
        assert resp.status_code == 200
        body = resp.json()[result_key]
        assert body["next_page"] is False
        assert len(body["results"]) == 0


@pytest.mark.asyncio
async def test_resource_search_pagination(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    kbid = knowledgebox

    n_texts = 20
    texts = [(f"text-{i}", f"Dummy text field to test ({i})") for i in range(n_texts)]

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        headers={"X-Synchronous": "true"},
        json={
            "title": "Resource with ",
            "slug": "resource-with-texts",
            "texts": {
                id: {
                    "body": text,
                    "format": "PLAIN",
                }
                for id, text in texts
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.type = BrokerMessage.AUTOCOMMIT
    bm.source = BrokerMessage.MessageSource.PROCESSOR

    for id, text in texts:
        bm.texts[id].body = text
        bm.texts[id].format = rpb.FieldText.Format.PLAIN

        etw = rpb.ExtractedTextWrapper()
        etw.field.field = id
        etw.field.field_type = rpb.FieldType.TEXT
        etw.body.text = text
        bm.extracted_text.append(etw)

        fcm = rpb.FieldComputedMetadataWrapper()
        paragraph = rpb.Paragraph(
            start=0, end=len(text), kind=rpb.Paragraph.TypeParagraph.TEXT
        )
        fcm.metadata.metadata.paragraphs.append(paragraph)
        fcm.field.field = id
        fcm.field.field_type = rpb.FieldType.TEXT
        bm.field_metadata.append(fcm)

    await inject_message(nucliadb_grpc, bm)

    total = n_texts
    page_size = 5
    total_pages = math.floor(total / page_size)
    query = "text"

    for page_number in range(0, total_pages):
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/resource/{rid}/search",
            params={
                "query": query,
                "features": ["paragraph"],
                "page_number": page_number,
                "page_size": page_size,
            },
        )
        assert resp.status_code == 200
        body = resp.json()["paragraphs"]
        assert body["next_page"] == (page_number != total_pages - 1)
        assert len(body["results"]) == body["page_size"] == page_size

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}/search",
        params={
            "query": query,
            "features": ["paragraph"],
            "page_number": page_number + 1,
            "page_size": page_size,
        },
    )
    assert resp.status_code == 200
    body = resp.json()["paragraphs"]
    assert body["next_page"] is False
    assert len(body["results"]) == 0


@pytest.mark.asyncio
async def test_search_handles_predict_errors(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    predict_mock,
):
    kbid = knowledgebox

    for convert_sentence_to_vector_mock in (
        AsyncMock(side_effect=PredictVectorMissing()),
        AsyncMock(side_effect=SendToPredictError()),
    ):
        predict_mock.convert_sentence_to_vector = convert_sentence_to_vector_mock
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/search",
            json={
                "features": ["vector"],
                "query": "something",
            },
        )
        assert resp.status_code == 206
        assert resp.reason_phrase == "Partial Content"
        predict_mock.convert_sentence_to_vector.assert_awaited_once()

    for detect_entities_mock in (AsyncMock(side_effect=SendToPredictError()),):
        predict_mock.detect_entities = detect_entities_mock
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/search",
            json={
                "features": ["relations"],
                "query": "something",
            },
        )
        assert resp.status_code == 200
        predict_mock.detect_entities.assert_awaited_once()
