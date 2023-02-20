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
import uuid
from datetime import datetime

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest.orm.resource import (
    add_field_classifications,
    remove_field_classifications,
)
from nucliadb.ingest.tests.vectors import V1, V2, V3
from nucliadb.tests.utils import inject_message
from nucliadb_models.common import UserClassification
from nucliadb_models.extracted import Classification
from nucliadb_models.metadata import (
    ComputedMetadata,
    FieldClassification,
    FieldID,
    ParagraphAnnotation,
    UserFieldMetadata,
)
from nucliadb_models.resource import Resource, ResourceList
from nucliadb_models.search import KnowledgeboxSearchResults
from nucliadb_models.text import TextField
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_protos import resources_pb2 as rpb


def broker_resource(knowledgebox: str) -> BrokerMessage:
    rid = str(uuid.uuid4())
    slug = f"{rid}slug1"

    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=slug,
        type=BrokerMessage.AUTOCOMMIT,
    )

    bm.basic.icon = "text/plain"
    bm.basic.title = "Title Resource"
    bm.basic.summary = "Summary of document"
    bm.basic.thumbnail = "doc"
    bm.basic.layout = "default"
    bm.basic.metadata.useful = True
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    bm.origin.source = rpb.Origin.Source.WEB

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer?"
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of document"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Title Resource"
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    c1 = rpb.Classification()
    c1.label = "label1"
    c1.labelset = "labelset1"

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    p1 = rpb.Paragraph(
        start=0,
        end=45,
    )
    p1.classifications.append(c1)
    p1.start_seconds.append(0)
    p1.end_seconds.append(10)
    p2 = rpb.Paragraph(
        start=47,
        end=64,
    )
    p2.classifications.append(c1)
    p2.start_seconds.append(10)
    p2.end_seconds.append(20)
    p2.start_seconds.append(20)
    p2.end_seconds.append(30)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.ner["Ramon"] = "PERSON"
    fcm.metadata.metadata.classifications.append(c1)

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

    v2 = rpb.Vector()
    v2.start = 20
    v2.end = 45
    v2.start_paragraph = 0
    v2.end_paragraph = 45
    v2.vector.extend(V2)
    ev.vectors.vectors.vectors.append(v2)

    v3 = rpb.Vector()
    v3.start = 48
    v3.end = 65
    v3.start_paragraph = 47
    v3.end_paragraph = 64
    v3.vector.extend(V3)
    ev.vectors.vectors.vectors.append(v3)

    bm.field_vectors.append(ev)
    bm.source = BrokerMessage.MessageSource.WRITER
    return bm


async def inject_resource_with_paragraph_labels(knowledgebox, writer):
    bm = broker_resource(knowledgebox)
    await inject_message(writer, bm)
    return bm.uuid


@pytest.mark.asyncio
async def test_labels_global(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc,
    knowledgebox,
):
    # PUBLIC API
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}")
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/labelset/label1",
        json={
            "title": "mylabel",
            "multiple": False,
            "labels": [{"title": "label1", "uri": "http://"}],
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/labelsets")
    assert resp.status_code == 200
    assert len(resp.json()["labelsets"]) == 1
    assert resp.json()["labelsets"]["label1"]["multiple"] is False

    rid = await inject_resource_with_paragraph_labels(knowledgebox, nucliadb_grpc)

    resp = await nucliadb_writer.post(f"/kb/{knowledgebox}/resource/{rid}/reindex")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_classification_labels_cancelled_by_the_user(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    expected_label = {
        "label": "label",
        "labelset": "labelset",
        "cancelled_by_user": True,
    }
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"X-SYNCHRONOUS": "TRUE"},
        json={
            "title": "My Resource",
            "summary": "My summary",
            "usermetadata": {"classifications": [expected_label]},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Check cancelled labels come in resource get
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}",
    )
    assert resp.status_code == 200
    content = resp.json()
    assert content["usermetadata"]["classifications"][0] == expected_label

    # Check cancelled labels come in resource list
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resources",
    )
    assert resp.status_code == 200
    content = resp.json()
    assert (
        content["resources"][0]["usermetadata"]["classifications"][0] == expected_label
    )

    # Check cancelled labels come in search results
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=summary")
    assert resp.status_code == 200
    content = resp.json()
    assert (
        content["resources"][rid]["usermetadata"]["classifications"][0]
        == expected_label
    )


@pytest.mark.asyncio
async def test_classification_labels_are_shown_in_resource_basic(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc,
    knowledgebox,
):
    rid = await inject_resource_with_paragraph_labels(knowledgebox, nucliadb_grpc)

    classifications = [Classification(labelset="labelset1", label="label1")]

    expected_computedmetadata = ComputedMetadata(
        field_classifications=[
            FieldClassification(
                field=FieldID(field="file", field_type=FieldID.FieldType.FILE),
                classifications=classifications,
            ),
        ]
    )

    # Check resource get
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}?show=basic")
    assert resp.status_code == 200
    resource = Resource.parse_raw(resp.content)
    assert resource.computedmetadata == expected_computedmetadata

    # Check resources list
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resources?show=basic")
    assert resp.status_code == 200
    resources = ResourceList.parse_raw(resp.content)
    assert resources.resources[0].computedmetadata == expected_computedmetadata

    # Check search results list
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?show=basic")
    assert resp.status_code == 200
    results = KnowledgeboxSearchResults.parse_raw(resp.content)
    assert results.resources[rid].computedmetadata == expected_computedmetadata


def test_remove_field_classifications():
    field = rpb.FieldID(field_type=rpb.FieldType.FILE, field="foo")
    basic = rpb.Basic()
    remove_field_classifications(basic, deleted_fields=[field])

    field = rpb.FieldID(field_type=rpb.FieldType.FILE, field="foo")
    basic.computedmetadata.field_classifications.append(
        rpb.FieldClassifications(field=field)
    )
    remove_field_classifications(basic, deleted_fields=[field])

    assert len(basic.computedmetadata.field_classifications) == 0


def test_add_field_classifications():
    field = rpb.FieldID(field_type=rpb.FieldType.FILE, field="foo")
    basic = rpb.Basic()

    fcmw = rpb.FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(field)

    assert add_field_classifications(basic, fcmw) is False

    assert len(basic.computedmetadata.field_classifications) == 0

    c1 = rpb.Classification(label="foo", labelset="bar")
    fcmw.metadata.metadata.classifications.append(c1)

    assert add_field_classifications(basic, fcmw) is True

    assert basic.computedmetadata.field_classifications[0] == rpb.FieldClassifications(
        field=field, classifications=[c1]
    )


@pytest.mark.asyncio
async def test_fieldmetadata_classification_labels(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    fieldmetadata = UserFieldMetadata(
        field=FieldID(field="text", field_type=FieldID.FieldType.TEXT),
        paragraphs=[
            ParagraphAnnotation(
                key="foobar",
                classifications=[
                    UserClassification(
                        label="foo", labelset="bar", cancelled_by_user=True
                    )
                ],
            )
        ],
    )
    payload = CreateResourcePayload(
        title="Foo",
        texts={"text": TextField(body="my text")},
        fieldmetadata=[fieldmetadata],
    )
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        data=payload.json(),  # type: ignore
        headers={"X-SYNCHRONOUS": "True"},
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Check resource get
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}?show=basic")
    assert resp.status_code == 200
    resource = Resource.parse_raw(resp.content)
    assert resource.fieldmetadata[0] == fieldmetadata  # type: ignore
