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
from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import AsyncClient
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.utilities import Utility, set_utility
from nucliadb.ingest.tests.vectors import V1
from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_utils.utilities import Utility, set_utility


@pytest.mark.asyncio
async def test_search_sc_2062(
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
        f"/kb/{knowledgebox}/search?query=Some&show=extracted&extracted=metadata"
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
async def test_search_can_filter_by_processing_status(
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

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "features": ["document"],
            "fields": ["a/title"],
        },
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == created

    for status in valid_status:
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/search",
            json={
                "features": ["document"],
                "fields": ["a/title"],
                "with_status": status,
            },
        )
        assert resp.status_code == 200
        assert len(resp.json()["resources"]) == 1

    # Check facets by processing status
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/search",
        json={
            "features": ["document"],
            "fields": ["a/title"],
            "faceted": ["/n/s"],
            "page_size": 0,
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

async def test_search_relations(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"X-SYNCHRONOUS": "True"},
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    relation_nodes = {
        "Animal": RelationNode(
            value="Animal", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Batman": RelationNode(
            value="Batman", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Becquer": RelationNode(
            value="Becquer", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Cat": RelationNode(
            value="Cat", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Catwoman": RelationNode(
            value="Catwoman", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Eric": RelationNode(
            value="Eric", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Fly": RelationNode(
            value="Fly", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Gravity": RelationNode(
            value="Gravity", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Joan Antoni": RelationNode(
            value="Joan Antoni", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Joker": RelationNode(
            value="Joker", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Newton": RelationNode(
            value="Newton", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Physics": RelationNode(
            value="Physics", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Poetry": RelationNode(
            value="Poetry", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Swallow": RelationNode(
            value="Swallow", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
    }

    relation_edges = [
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Batman"],
            to=relation_nodes["Catwoman"],
            relation_label="love",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Batman"],
            to=relation_nodes["Joker"],
            relation_label="fight",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Joker"],
            to=relation_nodes["Physics"],
            relation_label="enjoy",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Catwoman"],
            to=relation_nodes["Cat"],
            relation_label="imitate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Cat"],
            to=relation_nodes["Animal"],
            relation_label="species",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Newton"],
            to=relation_nodes["Physics"],
            relation_label="study",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Newton"],
            to=relation_nodes["Gravity"],
            relation_label="formulate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Eric"],
            to=relation_nodes["Cat"],
            relation_label="like",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Eric"],
            to=relation_nodes["Joan Antoni"],
            relation_label="collaborate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Joan Antoni"],
            to=relation_nodes["Becquer"],
            relation_label="read",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Becquer"],
            to=relation_nodes["Poetry"],
            relation_label="write",
        ),
        Relation(
            relation=Relation.RelationType.ABOUT,
            source=relation_nodes["Poetry"],
            to=relation_nodes["Swallow"],
            relation_label="about",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Swallow"],
            to=relation_nodes["Animal"],
            relation_label="species",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Swallow"],
            to=relation_nodes["Fly"],
            relation_label="can",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=relation_nodes["Fly"],
            to=relation_nodes["Gravity"],
            relation_label="defy",
        ),
    ]

    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = knowledgebox
    bm.relations.extend(relation_edges)
    await inject_message(nucliadb_grpc, bm)

    # Test search by relations
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
    assert resp.json()["relations"]["entities"] == {
        "Becquer": {
            "related_to": [
                {"entity": "Poetry", "relation": "write"},
                {"entity": "Becquer", "relation": "read"},
            ]
        },
        "Newton": {
            "related_to": [
                {"entity": "Physics", "relation": "study"},
                {"entity": "Gravity", "relation": "formulate"},
            ]
        },
    }
