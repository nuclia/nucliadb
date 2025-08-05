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


from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage, Generator
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


@pytest.mark.deploy_modes("standalone")
async def test_field_update_deletes_old_vectors(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
            "texts": {"text1": {"body": "Hello my dear friend"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.kbid = standalone_knowledgebox
    bm.uuid = rid

    field = FieldID(field_type=FieldType.TEXT, field="text1")
    etw = ExtractedTextWrapper()
    etw.body.text = "Hello my dear friend"
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(field)
    vector = Vector(start=0, end=len(etw.body.text), start_paragraph=0, end_paragraph=0)
    vector.vector.extend([1] * 512)
    evw.vectors.vectors.vectors.append(vector)
    bm.field_vectors.append(evw)

    await inject_message(nucliadb_ingest_grpc, bm)

    # should get 1 result
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={"query": "Hello", "features": ["semantic"], "min_score": -1},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    # Now, the field is edited to something else and there are no vectors computed
    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.kbid = standalone_knowledgebox
    bm.uuid = rid

    field = FieldID(field_type=FieldType.TEXT, field="text1")
    etw = ExtractedTextWrapper()
    etw.body.text = "zxcvb"
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(field)
    bm.field_vectors.append(evw)

    await inject_message(nucliadb_ingest_grpc, bm)

    # should get no results with semantic search
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "zxcvb",
            "features": ["semantic"],
            "min_score": -1,
            "rag_strategies": [{"name": "neighbouring_paragraphs"}],
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    print(resp.text)
    body = resp.json()
    assert len(body["retrieval_results"]["resources"]) == 0


@pytest.mark.deploy_modes("standalone")
async def test_field_with_many_entities(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
            "texts": {"text1": {"body": "Hello my dear friend"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.kbid = standalone_knowledgebox
    bm.uuid = rid

    field = FieldID(field_type=FieldType.TEXT, field="text1")
    etw = ExtractedTextWrapper()
    etw.body.text = "Hello my dear friend"
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "text1"
    fcmw.metadata.metadata.entities["processor"].entities.add(label="PERSON", text="Fry")
    fcmw.metadata.metadata.entities["processor"].entities.add(label="PERSON", text="Leela")
    fcmw.metadata.metadata.entities["processor"].entities.add(label="PERSON", text="Bender")
    fcmw.metadata.metadata.paragraphs.add(start=0, end=20)
    bm.field_metadata.append(fcmw)

    g = Generator()
    g.processor.SetInParent()
    bm.generated_by.append(g)

    with patch.object(cluster_settings, "max_entity_facets", 2):
        await inject_message(nucliadb_ingest_grpc, bm)

    # field is indexed
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={"query": "Hello", "min_score": -1},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    # two out of three entities are indexed
    indexed = []
    for entity in ["Fry", "Bender", "Leela"]:
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/find",
            json={
                "query": "Hello",
                "min_score": -1,
                "filter_expression": {"field": {"prop": "entity", "subtype": "PERSON", "value": entity}},
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        if len(body["resources"]) == 1:
            indexed.append(entity)

    assert len(indexed) == 2

    # Check a warning has been added
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={},
    )
    assert resp.status_code == 200
    body = resp.json()

    assert len(body["resources"]) == 1
    text = list(body["resources"].values())[0]["data"]["texts"]["text1"]
    assert text["status"] == "PROCESSED"
    assert len(text["errors"]) == 1
    assert "Too many detected entities" in text["errors"][0]["body"]
    assert text["errors"][0]["code_str"] == "INDEX"
    assert text["errors"][0]["severity"] == "WARNING"

    # Index again with unlimited entities
    await inject_message(nucliadb_ingest_grpc, bm)

    # Make sure the warning has been removed
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={},
    )
    assert resp.status_code == 200
    body = resp.json()

    assert len(body["resources"]) == 1
    text = list(body["resources"].values())[0]["data"]["texts"]["text1"]
    assert text["status"] == "PROCESSED"
    assert "errors" not in text


@pytest.mark.deploy_modes("standalone")
async def test_field_with_many_paragraphs(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
            "texts": {"text1": {"body": "Hello my dear friend"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.kbid = standalone_knowledgebox
    bm.uuid = rid

    field = FieldID(field_type=FieldType.TEXT, field="text1")
    etw = ExtractedTextWrapper()
    etw.body.text = "Hello my dear friend"
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "text1"
    fcmw.metadata.metadata.paragraphs.add(start=0, end=5)
    fcmw.metadata.metadata.paragraphs.add(start=5, end=10)
    fcmw.metadata.metadata.paragraphs.add(start=10, end=15)
    fcmw.metadata.metadata.paragraphs.add(start=15, end=20)
    bm.field_metadata.append(fcmw)

    g = Generator()
    g.processor.SetInParent()
    bm.generated_by.append(g)

    with patch.object(cluster_settings, "max_resource_paragraphs", 2):
        await inject_message(nucliadb_ingest_grpc, bm)

    # field is in error
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1
    resource = list(body["resources"].values())[0]
    assert resource["metadata"]["status"] == "ERROR"
    text = resource["data"]["texts"]["text1"]
    assert text["status"] == "ERROR"
    assert len(text["errors"]) == 1
    assert "Resource has too many paragraphs" in text["errors"][0]["body"]
    assert text["errors"][0]["code_str"] == "INDEX"
    assert text["errors"][0]["severity"] == "ERROR"

    # field is not indexed
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={"query": "Hello", "min_score": -1},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0

    # Index again with unlimited paragraphs
    await inject_message(nucliadb_ingest_grpc, bm)

    # Make sure the error has been removed
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1
    resource = list(body["resources"].values())[0]
    assert resource["metadata"]["status"] == "PROCESSED"
    text = resource["data"]["texts"]["text1"]
    assert text["status"] == "PROCESSED"
    assert "errors" not in text

    # field is indexed
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={"query": "Hello", "min_score": -1},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1
