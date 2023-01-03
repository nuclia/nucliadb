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

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import broker_resource, inject_message
from nucliadb_protos import resources_pb2 as rpb


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
async def test_search_advanced_query(
    nucliadb_reader: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    """
    Test description:
    - Searching with an invalid tantivy syntax should return an error
    - Searching with a valid tantivy advanced query should return expected results
    - Searching with advanceed query and a regular query should return the intersection
      of the results (ie: AND operation)
    """
    kbid = knowledgebox

    # Inject a couple of messages
    bm = broker_resource(kbid)
    bm.uuid = "barack"
    bm.basic.title = "barack obama"
    bm.basic.summary = "Barack was the president some time ago"
    await inject_message(nucliadb_grpc, bm)

    bm = broker_resource(kbid)
    bm.uuid = "trump"
    bm.basic.title = "donald trump"
    bm.basic.summary = "Donald Trump has also been a president in the past"
    await inject_message(nucliadb_grpc, bm)

    # Invalid advanced query
    invalid_advanced_query = "IN [unbalanced-parenthesis"
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "advanced_query": invalid_advanced_query,
        },
    )
    assert resp.status_code != 200

    # Valid advanced query
    advanced_query = 'uuid:"barack" OR uuid:"trump"'
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/search?advanced_query={advanced_query}"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["resources"]["barack"]
    assert resp_json["resources"]["trump"]

    # Advanced query + regular query should AND the results
    query_all = ""
    advanced_query = 'uuid:"barack"'
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/search",
        json={
            "query": query_all,
            "advanced_query": advanced_query,
        },
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    assert resp_json["resources"]["barack"]
