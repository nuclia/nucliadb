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
from typing import Optional
from uuid import uuid4

import pytest
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import Classification as PBClassification
from nucliadb_protos.resources_pb2 import ExtractedVectorsWrapper
from nucliadb_protos.resources_pb2 import FieldID as PBFieldID
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.resources_pb2 import Origin as PBOrigin
from nucliadb_protos.resources_pb2 import TokenSplit as PBTokenSplit
from nucliadb_protos.resources_pb2 import UserFieldMetadata as PBUserFieldMetadata
from nucliadb_protos.train_pb2 import EnabledMetadata
from nucliadb_protos.utils_pb2 import Relation as PBRelation
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    Classification,
    FieldComputedMetadataWrapper,
    FieldID,
    Paragraph,
)

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.tests.fixtures import create_resource
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import utils_pb2
from nucliadb_protos import utils_pb2 as upb


@pytest.mark.asyncio
async def test_create_resource_orm_with_basic(
    storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    basic = PBBasic(
        icon="text/plain",
        title="My title",
        summary="My summary",
        thumbnail="/file",
        layout="basic",
    )
    basic.metadata.metadata["key"] = "value"
    basic.metadata.language = "ca"
    basic.metadata.useful = True
    basic.metadata.status = PBMetadata.Status.PROCESSED

    cl1 = PBClassification(labelset="labelset1", label="label")
    basic.usermetadata.classifications.append(cl1)

    r1 = PBRelation(
        relation=PBRelation.CHILD,
        source=RelationNode(value="000000", ntype=RelationNode.NodeType.RESOURCE),
        to=RelationNode(value="000001", ntype=RelationNode.NodeType.RESOURCE),
    )

    basic.usermetadata.relations.append(r1)

    ufm1 = PBUserFieldMetadata(
        token=[PBTokenSplit(token="My home", klass="Location")],
        field=PBFieldID(field_type=FieldType.TEXT, field="title"),
    )

    basic.fieldmetadata.append(ufm1)
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug", basic=basic)
    assert r is not None

    b2: Optional[PBBasic] = await r.get_basic()
    assert b2 is not None
    assert b2.icon == "text/plain"

    o2: Optional[PBOrigin] = await r.get_origin()
    assert o2 is None

    o2 = PBOrigin()
    assert o2 is not None
    o2.source = PBOrigin.Source.API
    o2.source_id = "My Source"
    o2.created.FromDatetime(datetime.now())

    await r.set_origin(o2)
    o2 = await r.get_origin()
    assert o2 is not None
    assert o2.source_id == "My Source"


@pytest.mark.asyncio
async def test_iterate_paragraphs(
    storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    # Create a resource
    basic = PBBasic(
        icon="text/plain",
        title="My title",
        summary="My summary",
        thumbnail="/file",
        layout="basic",
    )
    basic.metadata.metadata["key"] = "value"
    basic.metadata.language = "ca"
    basic.metadata.useful = True
    basic.metadata.status = PBMetadata.Status.PROCESSED

    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug", basic=basic)
    assert r is not None

    # Add some labelled paragraphs to it
    bm = BrokerMessage()
    field1_if = FieldID()
    field1_if.field = "field1"
    field1_if.field_type = FieldType.TEXT
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(field1_if)
    p1 = Paragraph()
    p1.start = 0
    p1.end = 82
    p1.classifications.append(Classification(labelset="ls1", label="label1"))
    p2 = Paragraph()
    p2.start = 84
    p2.end = 103
    p2.classifications.append(Classification(labelset="ls1", label="label2"))
    fcmw.metadata.metadata.paragraphs.append(p1)
    fcmw.metadata.metadata.paragraphs.append(p2)
    bm.field_metadata.append(fcmw)
    await r.apply_extracted(bm)

    # Check iterate paragraphs
    async for paragraph in r.iterate_paragraphs(EnabledMetadata(labels=True)):
        assert len(paragraph.metadata.labels.paragraph) == 1
        assert paragraph.metadata.labels.paragraph[0].label in ("label1", "label2")


@pytest.mark.asyncio
async def test_vector_duplicate_fields(
    storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    basic = PBBasic(title="My title", summary="My summary")
    basic.metadata.status = PBMetadata.Status.PROCESSED

    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug", basic=basic)
    assert r is not None

    # Add some labelled paragraphs to it
    bm = BrokerMessage()
    field = FieldID(field="field1", field_type=FieldType.TEXT)
    fcmw = FieldComputedMetadataWrapper(field=field)
    p1 = Paragraph(
        start=0,
        end=82,
        classifications=[Classification(labelset="ls1", label="label1")],
    )
    p2 = Paragraph(
        start=84,
        end=103,
        classifications=[Classification(labelset="ls1", label="label2")],
    )
    fcmw.metadata.metadata.paragraphs.append(p1)
    fcmw.metadata.metadata.paragraphs.append(p2)
    bm.field_metadata.append(fcmw)
    bm.texts["field1"].body = "My text1"

    for i in range(5):
        bm.field_vectors.append(
            ExtractedVectorsWrapper(
                field=field,
                vectors=utils_pb2.VectorObject(
                    vectors=utils_pb2.Vectors(
                        vectors=[
                            utils_pb2.Vector(
                                start=0,
                                end=1,
                                start_paragraph=0,
                                end_paragraph=1,
                                vector=[0.1] * 768,
                            )
                        ]
                    )
                ),
            )
        )

    await r.apply_fields(bm)
    await r.apply_extracted(bm)

    count = 0
    for pkey1, para in r.indexer.brain.paragraphs.items():
        for pkey2, para2 in para.paragraphs.items():
            for key, sent in para2.sentences.items():
                count += 1
                assert (
                    len(sent.vector) == 768
                ), f"bad key {len(sent.vector)} {pkey1} - {pkey2} - {key}"

    assert count == 1


async def test_generate_broker_message(
    storage, maindb_driver, cache, fake_node, knowledgebox_ingest: str
):
    # Create a resource with all possible metadata in it
    resource = await create_resource(storage, maindb_driver, knowledgebox_ingest)

    # Now fetch it
    async with maindb_driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
        r = await kb_obj.get(resource.uuid)
        assert r is not None

    async with maindb_driver.transaction() as txn:
        r.txn = txn
        bm = await r.generate_broker_message()

    # Check generated broker message has the same metadata as the created resource

    # 1. ROOT ELEMENTS
    # 1.1 BASIC
    basic = bm.basic
    assert basic.HasField("created")
    assert basic.HasField("modified")
    assert basic.title == "My title"
    assert basic.summary == "My summary"
    assert basic.icon == "text/plain"
    assert basic.layout == "basic"
    assert basic.thumbnail == "/file"
    assert basic.last_seqid == 1
    assert basic.last_account_seq == 2
    basic_metadata = basic.metadata
    assert basic_metadata.metadata["key"] == "value"
    assert basic_metadata.language == "ca"
    assert basic_metadata.useful is True
    assert basic_metadata.status == rpb.Metadata.Status.PROCESSED
    basic_usermetadata = basic.usermetadata
    assert len(basic_usermetadata.classifications) == 1
    assert basic_usermetadata.classifications[0].label == "label1"
    assert basic_usermetadata.classifications[0].labelset == "labelset1"
    assert len(basic_usermetadata.relations) == 1
    assert basic_usermetadata.relations[0].to.value == "000001"
    basic_fieldmetadata = basic.fieldmetadata
    assert len(basic_fieldmetadata) == 1
    assert basic_fieldmetadata[0].field.field == "text1"
    assert basic_fieldmetadata[0].token[0].token == "My home"
    assert basic_fieldmetadata[0].token[0].klass == "Location"

    # 1.2 RELATIONS
    assert len(bm.relations) == 1
    assert bm.relations[0].relation == upb.Relation.CHILD
    assert bm.relations[0].source.value == resource.uuid
    assert bm.relations[0].source.ntype == upb.RelationNode.NodeType.RESOURCE
    assert bm.relations[0].to.value == "000001"
    assert bm.relations[0].to.ntype == upb.RelationNode.NodeType.RESOURCE

    # 1.3 ORIGIN
    assert bm.origin.source_id == "My Source"
    assert bm.origin.source == rpb.Origin.Source.API
    assert bm.origin.HasField("created")
    assert bm.origin.HasField("modified")

    # 2. FIELDS
    # 2.1 FILE FIELD
    file_field = bm.files["file1"]
    assert file_field.language == "es"
    assert file_field.HasField("added")
    assert file_field.file.uri
    assert file_field.file.filename == "text.pb"
    assert file_field.file.source == storage.source
    assert file_field.file.bucket_name
    assert file_field.file.size
    assert file_field.file.content_type == "application/octet-stream"
    assert file_field.file.filename == "text.pb"
    assert file_field.file.md5 == "01cca3f53edb934a445a3112c6caa652"

    # 2.2 LINK FIELD
    link_field = bm.links["link1"]
    assert link_field.uri == "htts://nuclia.cloud"
    assert link_field.language == "ca"
    assert link_field.HasField("added")
    assert link_field.headers["AUTHORIZATION"] == "Bearer xxxxx"

    assert len(bm.link_extracted_data) == 1
    led = bm.link_extracted_data[0]
    assert led.HasField("date")
    assert led.language == "ca"
    assert led.title == "My Title"
    assert led.field == "link1"
    assert led.HasField("link_preview")
    assert led.HasField("link_thumbnail")

    # Link extracted text
    letxt = [et for et in bm.extracted_text if et.field.field == "link1"][0]
    assert letxt.body.text == "MyText"

    # Link field computed metadata
    lfcm = [fcm for fcm in bm.field_metadata if fcm.field.field == "link1"][0]
    assert lfcm.metadata.metadata.links[0] == "https://nuclia.com"
    assert len(lfcm.metadata.metadata.paragraphs) == 1
    assert len(lfcm.metadata.metadata.positions) == 1
    assert lfcm.metadata.metadata.HasField("last_index")
    assert lfcm.metadata.metadata.HasField("last_understanding")
    assert lfcm.metadata.metadata.HasField("last_extract")
    assert lfcm.metadata.metadata.HasField("last_summary")
    assert lfcm.metadata.metadata.HasField("thumbnail")

    # Large field metadata
    llfm = [lfm for lfm in bm.field_large_metadata if lfm.field.field == "link1"][0]
    assert len(llfm.real.metadata.entities) == 2
    assert llfm.real.metadata.tokens["tok"] == 3

    # Field vectors
    lfv = [v for v in bm.field_vectors if v.field.field == "link1"][0]
    assert len(lfv.vectors.vectors.vectors) == 1
    assert lfv.vectors.vectors.vectors[0].start == 0
    assert lfv.vectors.vectors.vectors[0].end == 20
    assert lfv.vectors.vectors.vectors[0].vector == list(map(int, b"ansjkdn"))

    # 2.3 CONVERSATION FIELD
    conv_field = bm.conversations["conv1"]
    assert len(conv_field.messages) == 300
    for i, msg in enumerate(conv_field.messages):
        assert msg.HasField("timestamp")
        assert msg.who == "myself"
        assert msg.content.text == f"{i} hello"
        assert msg.content.format == rpb.MessageContent.Format.PLAIN
        if i == 33:
            assert len(msg.content.attachments) == 2

    # Extracted text
    cfet = [et for et in bm.extracted_text if et.field.field == "conv1"][0]
    assert cfet.body.text == "MyText"

    # TODO: Add checks for remaining field types and
    # other broker message metadata that is missing


async def test_generate_index_message_contains_all_metadata(
    storage, maindb_driver, cache, fake_node, knowledgebox_ingest: str
):
    # Create a resource with all possible metadata in it
    resource = await create_resource(storage, maindb_driver, knowledgebox_ingest)
    resource.disable_vectors = False

    async with maindb_driver.transaction() as txn:
        resource.txn = txn  # I don't like this but this is the API we have...
        resource_brain = await resource.generate_index_message()
    index_message = resource_brain.brain

    # Global resource labels
    assert set(index_message.labels) == {
        "/l/labelset1/label1",
        "/n/i/text/plain",
        "/s/p/ca",
        "/u/s/My Source",
        "/o/My Source",
        "/n/s/PROCESSED",
    }

    # Check texts are populated with field extracted text and field computed labels
    expected_fields = {
        "a/title",
        "a/summary",
        "u/link1",
        "d/datetime1",
        "c/conv1",
        "f/file1",
        "t/text1",
        "k/keywordset1",
        "l/layout1",
    }
    fields_to_be_found = expected_fields.copy()
    for field, text in index_message.texts.items():
        assert field in fields_to_be_found
        fields_to_be_found.remove(field)
        assert text.text == "MyText"
        assert {"/l/labelset1/label1", "/e/ENTITY/document"}.issubset(set(text.labels))
        if field in ("u/link", "t/text1"):
            assert "/e/Location/My home" in text.labels

    assert len(fields_to_be_found) == 0

    # Metadata
    assert index_message.metadata.created.seconds > 0
    assert index_message.metadata.modified.seconds > 0
    assert (
        index_message.metadata.modified.seconds
        >= index_message.metadata.created.seconds
    )

    # Processing status
    assert index_message.status == Resource.ResourceStatus.PROCESSED

    # Paragraphs
    assert set(index_message.paragraphs.keys()) == expected_fields
    for field, field_paragraphs in index_message.paragraphs.items():
        for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
            # Check that the key is correct
            parts = paragraph_id.split("/")
            uuid = parts[0]
            field_id = "/".join(parts[1:3])
            start, end = map(int, parts[-1].split("-"))
            assert uuid == resource.uuid
            assert field_id == field
            assert start == paragraph.metadata.position.start == paragraph.start
            assert end == paragraph.metadata.position.end == paragraph.end
            assert start <= end

    # relations
    assert len(index_message.relations) > 0

    # vectors in vectorset
    assert len(index_message.vectors) == 1
    vector = index_message.vectors["vectorset1"].vectors.popitem()[1].vector
    assert len(vector) > 0
