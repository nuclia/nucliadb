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
import os
from datetime import datetime, timedelta
from unittest import mock
from unittest.mock import AsyncMock, Mock, patch

import nats
import pytest
from httpx import AsyncClient
from nats.aio.client import Client
from nats.js import JetStreamContext

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.consumer import shard_creator
from nucliadb.search.predict import SendToPredictError
from nucliadb.tests.vectors import V1
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos.audit_pb2 import AuditRequest, ClientType
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_storage,
    set_utility,
)
from tests.utils import broker_resource, inject_message

TESTING_MAINDB_DRIVERS = os.environ.get("TESTING_MAINDB_DRIVERS", "pg,local").split(",")


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


async def create_resource_with_duplicates(knowledgebox, writer: WriterStub, sentence: str):
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
    await create_resource_with_duplicates(knowledgebox, nucliadb_grpc, sentence="My own text Ramon. ")
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
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query={query}&with_duplicates=false")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["paragraphs"]["results"]) == 2

    # It should return duplicates if specified
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query={query}&with_duplicates=true")
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
    await create_resource_with_duplicates(knowledgebox, nucliadb_grpc, sentence=sentence)
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
        f"/kb/{knowledgebox}/search?query=Some",
    )
    assert resp.status_code == 200
    content = resp.json()
    assert content["paragraphs"]["results"]
    par = content["paragraphs"]["results"][0]
    assert par["labels"] == ["labelset1/label2", "labelset1/label1"]


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
        f"/kb/{knowledgebox}/search?show=basic&filters=/icon/application/pdf"
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1

    # With a different icon should return no results
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search?show=basic&filters=/icon/application/docx"
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
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}/search?query=mushrooms")
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
@pytest.mark.skipif("pg" in TESTING_MAINDB_DRIVERS, reason="PG catalog does not support with_status")
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
            "filters": ["/metadata.status/PENDING"],
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1

    # Check facets by processing status
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "faceted": ["/metadata.status"],
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    facets = resp_json["fulltext"]["facets"]
    for status in valid_status:
        assert facets["/metadata.status"][f"/metadata.status/{status}"] == 1


@pytest.mark.skip(reason="Needs sc-5626")
@pytest.mark.asyncio
async def test_(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "Rust for dummies",
        },
    )
    assert resp.status_code == 201
    rust_for_dummies = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "Introduction to Python",
        },
    )
    assert resp.status_code == 201
    intro_to_python = resp.json()["uuid"]

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "Rust",
        },
    )
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == 1
    assert rust_for_dummies in resources

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "Intro",
        },
    )
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == 1
    assert intro_to_python in resources


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


def get_resource_with_a_sentence(knowledgebox):
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
    return bm


async def inject_resource_with_a_sentence(knowledgebox, writer):
    bm = get_resource_with_a_sentence(knowledgebox)
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
        assert len(entities[entity]["related_to"]) == len(expected[entity]["related_to"])

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
        assert len(entities[entity]["related_to"]) == len(expected[entity]["related_to"])

        for expected_relation in expected[entity]["related_to"]:
            assert expected_relation in entities[entity]["related_to"]


@pytest.mark.asyncio
@pytest.mark.skipif("pg" in TESTING_MAINDB_DRIVERS, reason="PG catalog does not support with_status")
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
            "faceted": ["/metadata.status"],
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    facets = resp_json["fulltext"]["facets"]
    assert facets["/metadata.status"] == {"/metadata.status/PENDING": 1}


@pytest.mark.asyncio
async def test_search_automatic_relations(
    nucliadb_reader: AsyncClient, nucliadb_writer: AsyncClient, knowledgebox
):
    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
            "origin": {
                "collaborators": ["Anne", "John"],
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
        assert len(entities[entity]["related_to"]) == len(expected[entity]["related_to"])

        assert sorted(expected[entity]["related_to"], key=lambda x: x["entity"]) == sorted(
            entities[entity]["related_to"], key=lambda x: x["entity"]
        )

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
        assert len(entities[entity]["related_to"]) == len(expected[entity]["related_to"])

        assert sorted(expected[entity]["related_to"], key=lambda x: x["entity"]) == sorted(
            entities[entity]["related_to"], key=lambda x: x["entity"]
        )


async def get_audit_messages(sub):
    msg = await sub.fetch(1)
    auditreq = AuditRequest()
    auditreq.ParseFromString(msg[0].data)
    return auditreq


@pytest.mark.asyncio
async def test_search_sends_audit(
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
    subject = audit_settings.audit_jetstream_target.format(partition=partition, type="*")

    set_utility(Utility.AUDIT, stream_audit)

    try:
        await jetstream.delete_stream(name=audit_settings.audit_stream)
    except nats.js.errors.NotFoundError:
        pass

    await jetstream.add_stream(name=audit_settings.audit_stream, subjects=[subject])
    psub = await jetstream.pull_subscribe(subject, "psub")

    # Test search sends audit
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search", headers={"x-ndb-client": "chrome_extension"})
    assert resp.status_code == 200

    auditreq = await get_audit_messages(psub)

    assert auditreq.kbid == kbid
    assert auditreq.type == AuditRequest.AuditType.SEARCH
    assert auditreq.client_type == ClientType.CHROME_EXTENSION  # Just to use other that the enum default
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
        paragraph = rpb.Paragraph(start=0, end=len(text), kind=rpb.Paragraph.TypeParagraph.TEXT)
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
@pytest.mark.parametrize("endpoint", ["search", "find"])
async def test_search_endpoints_handle_predict_errors(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    predict_mock,
    endpoint,
):
    kbid = knowledgebox

    for query_mock in (AsyncMock(side_effect=SendToPredictError()),):
        predict_mock.query = query_mock
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/{endpoint}",
            json={
                "features": ["vector"],
                "query": "something",
            },
        )
        assert resp.status_code == 206, resp.text
        assert resp.reason_phrase == "Partial Content"
        predict_mock.query.assert_awaited_once()


async def create_dummy_resources(
    nucliadb_writer: AsyncClient, nucliadb_grpc: WriterStub, kbid, n=10, start=0
):
    payloads = [
        {
            "slug": f"dummy-resource-{i}",
            "title": f"Dummy resource {i}",
            "summary": f"Dummy resource {i} summary",
        }
        for i in range(start, n)
    ]
    for payload in payloads:
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json=payload,
        )
        assert resp.status_code == 201
        uuid = resp.json()["uuid"]

        # Inject a vector
        message = BrokerMessage()
        message.kbid = kbid
        message.uuid = uuid
        message.texts["text"].body = "My own text Ramon. This is great to be here. \n Where is my beer?"
        etw = rpb.ExtractedTextWrapper()
        etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer?"
        etw.field.field = "text"
        etw.field.field_type = rpb.FieldType.FILE
        message.extracted_text.append(etw)
        ev = rpb.ExtractedVectorsWrapper()
        ev.field.field = "text"
        ev.field.field_type = rpb.FieldType.FILE
        v1 = rpb.Vector()
        v1.start = 0
        v1.end = 19
        v1.start_paragraph = 0
        v1.end_paragraph = 45
        v1.vector.extend(V1)
        ev.vectors.vectors.vectors.append(v1)
        message.field_vectors.append(ev)
        message.source = BrokerMessage.MessageSource.PROCESSOR

        await inject_message(nucliadb_grpc, message)


@pytest.fixture(scope="function")
async def kb_with_one_logic_shard(
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
):
    resp = await nucliadb_manager.post("/kbs", json={})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    await create_dummy_resources(nucliadb_writer, nucliadb_grpc, kbid, n=10)

    yield kbid

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
def max_shard_paragraphs():
    with patch.object(cluster_settings, "max_shard_paragraphs", 20):
        yield


@pytest.fixture(scope="function")
async def kb_with_two_logic_shards(
    max_shard_paragraphs,
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
):
    sc = shard_creator.ShardCreatorHandler(
        driver=get_driver(),
        storage=await get_storage(),
        pubsub=None,  # type: ignore
    )
    resp = await nucliadb_manager.post("/kbs", json={})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    await create_dummy_resources(nucliadb_writer, nucliadb_grpc, kbid, n=8)

    # trigger creating new shard manually here
    sc.shard_manager.should_create_new_shard = Mock(return_value=True)  # type: ignore
    await sc.process_kb(kbid)

    await create_dummy_resources(nucliadb_writer, nucliadb_grpc, kbid, n=10, start=8)

    yield kbid

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200


@pytest.mark.flaky(reruns=5)
@pytest.mark.asyncio
async def test_search_two_logic_shards(
    nucliadb_reader: AsyncClient,
    nucliadb_manager: AsyncClient,
    kb_with_one_logic_shard,
    kb_with_two_logic_shards,
):
    kbid1 = kb_with_one_logic_shard
    kbid2 = kb_with_two_logic_shards

    # Check that they have one and two logic shards, respectively
    resp = await nucliadb_manager.get(f"kb/{kbid1}/shards")
    assert resp.status_code == 200
    assert len(resp.json()["shards"]) == 1

    resp = await nucliadb_manager.get(f"kb/{kbid2}/shards")
    assert resp.status_code == 200
    assert len(resp.json()["shards"]) == 2

    # Check that search returns the same results
    resp1 = await nucliadb_reader.post(
        f"/kb/{kbid1}/search",
        json=dict(query="dummy", vector=V1, min_score={"semantic": -1}, with_duplicates=True),
    )
    resp2 = await nucliadb_reader.post(
        f"/kb/{kbid2}/search",
        json=dict(query="dummy", vector=V1, min_score={"semantic": -1}, with_duplicates=True),
    )
    assert resp1.status_code == resp2.status_code == 200
    content1 = resp1.json()
    content2 = resp2.json()

    assert len(content1["shards"]) == 1
    assert len(content2["shards"]) == 2

    assert len(content1["paragraphs"]["results"]) == len(content2["paragraphs"]["results"]) == 20

    assert len(content1["sentences"]["results"]) == len(content2["sentences"]["results"])


async def test_search_min_score(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    # When not specifying the min score on the request, it should default to 0.7
    resp = await nucliadb_reader.post(f"/kb/{knowledgebox}/search", json={"query": "dummy"})
    assert resp.status_code == 200
    assert resp.json()["sentences"]["min_score"] == 0.7

    # If we specify a min score, it should be used
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={"query": "dummy", "min_score": {"bm25": 10, "semantic": 0.5}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["sentences"]["min_score"] == 0.5
    assert body["paragraphs"]["min_score"] == 10
    assert body["fulltext"]["min_score"] == 10


@pytest.mark.parametrize(
    "facets,valid,error_message",
    [
        (
            [f"/a/{i}" for i in range(51)],
            False,
            "should have at most 50 items",
        ),
        (
            ["/a/b", "/a/b"],
            False,
            "Facet /a/b is already present in facets. Faceted list must be unique.",
        ),
        (
            ["/a/b", "/a/b/c"],
            False,
            "Nested facets are not allowed: /a/b/c is a child of /a/b",
        ),
        (["/a/b", "/c/d/e", "/f/g"], True, ""),
        (["/a/b", "/a/be"], True, ""),
        (["/a/b", "/a/be"], True, ""),
    ],
)
async def test_facets_validation(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    facets,
    valid,
    error_message,
):
    kbid = knowledgebox
    for endpoint in ("search",):
        for method in ("post", "get"):
            func = getattr(nucliadb_reader, method)
            kwargs = (
                {"params": {"faceted": facets}} if method == "get" else {"json": {"faceted": facets}}
            )
            resp = await func(f"/kb/{kbid}/{endpoint}", **kwargs)
            if valid:
                assert resp.status_code == 200
            else:
                assert resp.status_code == 422
                assert error_message in resp.json()["detail"][0]["msg"]


@pytest.mark.asyncio
async def test_search_marks_fuzzy_results(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
        },
    )
    assert resp.status_code == 201

    # Should get only one non-fuzzy result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "query": "Title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=False, n_expected=1)

    # Should get only one fuzzy result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "query": "totle",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=True, n_expected=1)

    # Should not get any result if exact match term queried
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "query": '"totle"',
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=True, n_expected=0)


def check_fuzzy_paragraphs(search_response, *, fuzzy_result: bool, n_expected: int):
    found = 0
    for paragraph in search_response["paragraphs"]["results"]:
        assert paragraph["fuzzy_result"] is fuzzy_result
        found += 1
    assert found == n_expected


@pytest.mark.asyncio
async def test_search_by_path_filter(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    paths = ["/foo", "foo/bar", "foo/bar/1", "foo/bar/2", "foo/bar/3", "foo/bar/4"]

    for path in paths:
        resp = await nucliadb_writer.post(
            f"/kb/{knowledgebox}/resources",
            json={
                "title": f"My resource: {path}",
                "summary": "Some summary",
                "origin": {
                    "path": path,
                },
            },
        )
        assert resp.status_code == 201

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == len(paths)

    # Get the list of all
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?filters=/origin.path/foo")
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == len(paths)

    # Get the list of under foo/bar
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?filters=/origin.path/foo/bar")
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == len(paths) - 1

    # Get the list of under foo/bar/4
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?filters=/origin.path/foo/bar/4")
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1


@pytest.mark.asyncio
async def test_search_kb_not_found(nucliadb_reader: AsyncClient):
    resp = await nucliadb_reader.get(
        "/kb/00000000000000/search?query=own+text",
    )
    assert resp.status_code == 404


@pytest.mark.asyncio()
async def test_resource_search_query_param_is_optional(nucliadb_reader, knowledgebox):
    kb = knowledgebox
    # If query is not present, should not fail
    resp = await nucliadb_reader.get(f"/kb/{kb}/search")
    assert resp.status_code == 200

    # Less than 3 characters should not fail either
    for query in ("f", "fo"):
        resp = await nucliadb_reader.get(f"/kb/{kb}/search?query={query}")
        assert resp.status_code == 200


@pytest.mark.asyncio()
async def test_search_with_duplicates(nucliadb_reader, knowledgebox):
    kb = knowledgebox
    resp = await nucliadb_reader.get(f"/kb/{kb}/search?with_duplicates=True")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kb}/search?with_duplicates=False")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
def search_with_limits_exceeded_error():
    with mock.patch(
        "nucliadb.search.api.v1.search.search",
        side_effect=LimitsExceededError(402, "over the quota"),
    ):
        yield


@pytest.mark.asyncio()
async def test_search_handles_limits_exceeded_error(
    nucliadb_reader, knowledgebox, search_with_limits_exceeded_error
):
    kb = knowledgebox
    resp = await nucliadb_reader.get(f"/kb/{kb}/search")
    assert resp.status_code == 402
    assert resp.json() == {"detail": "over the quota"}

    resp = await nucliadb_reader.post(f"/kb/{kb}/search", json={})
    assert resp.status_code == 402
    assert resp.json() == {"detail": "over the quota"}


@pytest.mark.asyncio
@pytest.mark.skipif("pg" in TESTING_MAINDB_DRIVERS, reason="PG catalog cannot do shards")
async def test_catalog_returns_shard_and_node_data(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["shards"]) > 0

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "query": "",
            "debug": "true",
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["nodes"]) > 0


@pytest.mark.asyncio
async def test_catalog_post(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/catalog",
        json={
            "query": "",
            "filters": [
                {"any": ["/foo", "/bar"]},
            ],
            "with_status": "PROCESSED",
            "sort": {
                "field": "created",
            },
        },
    )
    assert resp.status_code == 200


@pytest.fixture()
def not_debug():
    from nucliadb_utils.settings import running_settings

    prev = running_settings.debug
    running_settings.debug = False
    yield
    running_settings.debug = prev


async def test_api_does_not_show_tracebacks_on_api_errors(not_debug, nucliadb_reader: AsyncClient):
    with mock.patch(
        "nucliadb.search.api.v1.search.search",
        side_effect=Exception("Something went wrong"),
    ):
        resp = await nucliadb_reader.get("/kb/foobar/search", timeout=None)
        assert resp.status_code == 500
        assert resp.json() == {"detail": "Something went wrong, please contact your administrator"}


@pytest.mark.asyncio
async def test_catalog_pagination(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    n_resources = 35
    for i in range(n_resources):
        resp = await nucliadb_writer.post(
            f"/kb/{knowledgebox}/resources",
            json={
                "title": f"Resource {i}",
                "texts": {
                    "text": {
                        "body": f"Text for resource {i}",
                    }
                },
            },
        )
        assert resp.status_code == 201

    # Give some time for the resources to be refreshed in the index
    await asyncio.sleep(1)

    resource_uuids = []
    creation_dates = []
    page_size = 10
    page_number = 0
    while True:
        resp = await nucliadb_reader.get(
            f"/kb/{knowledgebox}/catalog",
            params={
                "page_number": page_number,
                "page_size": page_size,
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) <= page_size
        assert body["fulltext"]["page_number"] == page_number
        for resource_id, resource_data in body["resources"].items():
            resource_created_date = datetime.fromisoformat(resource_data["created"]).timestamp()
            if resource_id in resource_uuids:
                assert False, f"Resource {resource_id} already seen"
            resource_uuids.append(resource_id)
            creation_dates.append(resource_created_date)
        if not body["fulltext"]["next_page"]:
            break
        page_number += 1
    assert len(resource_uuids) == n_resources


@pytest.mark.asyncio
async def test_catalog_date_range_filtering(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    now = datetime.now()
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": f"Resource",
            "texts": {
                "text": {
                    "body": f"Text for resource",
                }
            },
        },
    )
    assert resp.status_code == 201

    one_hour_ago = now - timedelta(hours=1)
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
        params={
            "range_creation_start": one_hour_ago.isoformat(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/catalog",
        json={
            "range_creation_end": one_hour_ago.isoformat(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0


@pytest.mark.asyncio
async def test_catalog_faceted(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    valid_status = ["PROCESSED", "PENDING", "ERROR"]

    for status_name, status_value in rpb.Metadata.Status.items():
        if status_name not in valid_status:
            continue
        bm = broker_resource(knowledgebox)
        bm.basic.metadata.status = status_value
        await inject_message(nucliadb_grpc, bm)

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog?faceted=/metadata.status",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 3
    facets = body["fulltext"]["facets"]["/metadata.status"]
    assert len(facets) == 3
    for facet, count in facets.items():
        assert facet.split("/")[-1] in valid_status
        assert count == 1


@pytest.mark.asyncio
async def test_catalog_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    valid_status = ["PROCESSED", "PENDING", "ERROR"]

    for status_name, status_value in rpb.Metadata.Status.items():
        if status_name not in valid_status:
            continue
        bm = broker_resource(knowledgebox)
        bm.basic.metadata.status = status_value
        await inject_message(nucliadb_grpc, bm)

    # No filters
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 3

    # Simple filter
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/catalog?filters=/metadata.status/PENDING",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1
    assert list(body["resources"].values())[0]["metadata"]["status"] == "PENDING"

    # AND filter
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/catalog",
        json={"filters": [{"all": ["/metadata.status/PENDING", "/metadata.status/ERROR"]}]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0

    # OR filter
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/catalog",
        json={"filters": [{"any": ["/metadata.status/PENDING", "/metadata.status/ERROR"]}]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 2
    for resource in body["resources"].values():
        assert resource["metadata"]["status"] in ["PENDING", "ERROR"]
