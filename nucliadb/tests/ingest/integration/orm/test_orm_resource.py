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

from nidx_protos.noderesources_pb2 import Resource

from nucliadb.common import datamanagers
from nucliadb.ingest.orm.broker_message import generate_broker_message
from nucliadb.ingest.orm.index_message import get_resource_index_message
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import utils_pb2
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import Classification as PBClassification
from nucliadb_protos.resources_pb2 import ExtractedVectorsWrapper, FieldType
from nucliadb_protos.resources_pb2 import FieldID as PBFieldID
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.resources_pb2 import Origin as PBOrigin
from nucliadb_protos.resources_pb2 import UserFieldMetadata as PBUserFieldMetadata
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    Classification,
    FieldComputedMetadataWrapper,
    FieldID,
    Paragraph,
)
from tests.ndbfixtures.ingest import create_resource


async def test_create_resource_orm_with_basic(
    storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    basic = PBBasic(
        icon="text/plain",
        title="My title",
        summary="My summary",
        thumbnail="/file",
    )
    basic.metadata.metadata["key"] = "value"
    basic.metadata.language = "ca"
    basic.metadata.useful = True
    basic.metadata.status = PBMetadata.Status.PROCESSED

    cl1 = PBClassification(labelset="labelset1", label="label")
    basic.usermetadata.classifications.append(cl1)

    ufm1 = PBUserFieldMetadata(
        paragraphs=[rpb.ParagraphAnnotation(classifications=[cl1], key="key1")],
        field=PBFieldID(field_type=FieldType.TEXT, field="title"),
    )

    basic.fieldmetadata.append(ufm1)
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
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


async def test_paragraphs_with_page(storage, txn, cache, dummy_nidx_utility, knowledgebox: str):
    # Create a resource
    basic = PBBasic(
        icon="text/plain",
        title="My title",
        summary="My summary",
        thumbnail="/file",
    )
    basic.metadata.metadata["key"] = "value"
    basic.metadata.language = "ca"
    basic.metadata.useful = True
    basic.metadata.status = PBMetadata.Status.PROCESSED

    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
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
    p1.page.page = 3
    p1.page.page_with_visual = True
    p1.representation.is_a_table = True
    p1.representation.reference_file = "myfile"
    p2 = Paragraph()
    p2.start = 84
    p2.end = 103
    p2.classifications.append(Classification(labelset="ls1", label="label2"))
    fcmw.metadata.metadata.paragraphs.append(p1)
    fcmw.metadata.metadata.paragraphs.append(p2)
    bm.field_metadata.append(fcmw)
    await r.apply_extracted(bm)
    index_message = await get_resource_index_message(r, reindex=False)
    for metadata in index_message.paragraphs["t/field1"].paragraphs.values():
        if metadata.start == 84:
            assert metadata.metadata.position.in_page is False
            assert metadata.metadata.position.page_number == 0
            assert metadata.metadata.page_with_visual is False
            assert metadata.metadata.representation.is_a_table is False
            assert metadata.metadata.representation.file == ""

        if metadata.start == 0:
            assert metadata.metadata.position.in_page is True
            assert metadata.metadata.position.page_number == 3
            assert metadata.metadata.page_with_visual is True
            assert metadata.metadata.representation.is_a_table is True
            assert metadata.metadata.representation.file == "myfile"


async def test_vector_duplicate_fields(
    maindb_driver, storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    basic = PBBasic(title="My title", summary="My summary")
    basic.metadata.status = PBMetadata.Status.PROCESSED
    vector_dimension = 768

    kbid = KnowledgeBox.new_unique_kbid()
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=f"slug-{kbid}",
        semantic_models={
            "my-model": SemanticModelMetadata(
                similarity_function=utils_pb2.VectorSimilarity.COSINE,
                vector_dimension=vector_dimension,
            )
        },
    )
    async with datamanagers.with_rw_transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=kbid)

        basic = PBBasic(title="My title", summary="My summary")
        basic.metadata.status = PBMetadata.Status.PROCESSED

        rid = str(uuid4())
        resource = await kb_obj.add_resource(uuid=rid, slug="slug", basic=basic)
        assert resource is not None

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
                                    vector=[0.1] * vector_dimension,
                                )
                            ]
                        )
                    ),
                )
            )

        await resource.apply_fields(bm)
        await resource.apply_extracted(bm)
        index_message = await get_resource_index_message(resource, reindex=False)
        await txn.commit()

    count = 0
    for field_id, field_paragraphs in index_message.paragraphs.items():
        for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
            for vectorset_id, vectorset_sentences in paragraph.vectorsets_sentences.items():
                for vector_id, sentence in vectorset_sentences.sentences.items():
                    count += 1
                    assert len(sentence.vector) == 768, (
                        f"bad key {len(sentence.vector)} {field_id} - {paragraph_id} - {vectorset_id} - {vector_id}"
                    )

    assert count == 1


async def test_generate_broker_message(
    storage, maindb_driver, cache, dummy_nidx_utility, knowledgebox: str
):
    # Create a resource with all possible metadata in it
    full_resource = await create_resource(storage, maindb_driver, knowledgebox)

    # Now fetch it
    async with maindb_driver.ro_transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
        resource = await kb_obj.get(full_resource.uuid)
        assert resource is not None

    async with maindb_driver.ro_transaction() as txn:
        resource.txn = txn
        bm = await generate_broker_message(resource)

    # Check generated broker message has the same metadata as the created resource

    # 1. ROOT ELEMENTS
    # 1.1 BASIC
    basic = bm.basic
    assert basic.HasField("created")
    assert basic.HasField("modified")
    assert basic.title == "My title"
    assert basic.summary == "My summary"
    assert basic.icon == "text/plain"
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
    basic_fieldmetadata = basic.fieldmetadata
    assert len(basic_fieldmetadata) == 1
    assert basic_fieldmetadata[0].field.field == "text1"

    # 1.2 USER RELATIONS
    assert len(bm.user_relations.relations) == 1
    assert bm.user_relations.relations[0].to.value == "000001"

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
    assert len(lfcm.metadata.metadata.entities["processor"].entities) == 1
    assert len(lfcm.metadata.metadata.entities["my-task-id"].entities) == 1

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
    idx = 0
    async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kb_obj.kbid):
        lfv = [
            v for v in bm.field_vectors if v.field.field == "link1" and v.vectorset_id == vectorset_id
        ][0]
        assert len(lfv.vectors.vectors.vectors) == 1
        assert lfv.vectors.vectors.vectors[0].start == 0
        assert lfv.vectors.vectors.vectors[0].end == 20
        assert (
            lfv.vectors.vectors.vectors[0].vector
            == [float(idx)] * vs.vectorset_index_config.vector_dimension
        )
        idx += 1

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
    storage, maindb_driver, cache, dummy_nidx_utility, knowledgebox: str
):
    # Create a resource with all possible metadata in it
    resource = await create_resource(storage, maindb_driver, knowledgebox)
    resource.disable_vectors = False

    async with maindb_driver.ro_transaction() as txn:
        resource.txn = txn  # I don't like this but this is the API we have...
        index_message = await get_resource_index_message(resource, reindex=False)

    # Global resource labels
    assert set(index_message.labels) == {
        "/l/labelset1/label1",
        "/n/i/text/plain",
        "/s/p/ca",
        "/u/s/My Source",
        "/o/My Source",
        "/n/s/PROCESSED",
    }
    # Make sure there are no duplicates
    assert len(index_message.labels) == len(set(index_message.labels))

    # Check that field labels contain the right set of labels
    for text_info in index_message.texts.values():
        assert "/mt/text/html" in text_info.labels

    # Check texts are populated with field extracted text and field computed labels
    expected_fields = {
        "a/title",
        "a/summary",
        "u/link1",
        "c/conv1",
        "f/file1",
        "t/text1",
    }
    fields_to_be_found = expected_fields.copy()
    for field, text_info in index_message.texts.items():
        assert field in fields_to_be_found
        fields_to_be_found.remove(field)
        assert text_info.text == "MyText"
        assert {
            "/l/labelset1/label1",
            "/e/ENTITY/document",
            "/e/NOUN/document",
        }.issubset(set(text_info.labels))
    assert len(fields_to_be_found) == 0

    # Metadata
    assert index_message.metadata.created.seconds > 0
    assert index_message.metadata.modified.seconds > 0
    assert index_message.metadata.modified.seconds >= index_message.metadata.created.seconds

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
    assert len(index_message.field_relations["a/metadata"].relations) > 0

    # TODO: Uncomment when vectorsets is implemented
    # # vectors in vectorset
    # assert len(index_message.vectors) == 1
    # vector = index_message.vectors["vectorset1"].vectors.popitem()[1].vector
    # assert len(vector) > 0


async def test_generate_index_message_vectorsets(
    storage, maindb_driver, cache, dummy_nidx_utility, knowledgebox_with_vectorsets: str
):
    # Create a resource with all possible metadata in it
    resource = await create_resource(storage, maindb_driver, knowledgebox_with_vectorsets)
    resource.disable_vectors = False

    async with maindb_driver.ro_transaction() as txn:
        resource.txn = txn  # I don't like this but this is the API we have...
        index_message = await get_resource_index_message(resource, reindex=False)

    # Check length of vectorsets of first sentence of first paragraph. In the fixture, we set the vector
    # to be equal to the vectorset index, repeated to its length, to be able to differentiate
    vectorsets = {}
    async with datamanagers.utils.with_ro_transaction() as txn:
        idx = 0.0
        async for _, vs in datamanagers.vectorsets.iter(txn, kbid=knowledgebox_with_vectorsets):
            vectorsets[vs.vectorset_id] = (vs, idx)
            idx += 1

    for field in index_message.paragraphs.values():
        for paragraph in field.paragraphs.values():
            # assert len(paragraph.vectorsets_sentences) == len(vectorsets)
            for vectorset_id, vs_sentences in paragraph.vectorsets_sentences.items():
                config_dimension = vectorsets[vectorset_id][0].vectorset_index_config.vector_dimension
                vectorset_index = vectorsets[vectorset_id][1]
                for sentence in vs_sentences.sentences.values():
                    assert len(sentence.vector) == config_dimension
                    assert sentence.vector == [vectorset_index] * config_dimension


async def test_generate_index_message_cancels_labels(
    storage, maindb_driver, cache, dummy_nidx_utility, knowledgebox_with_vectorsets: str
):
    # Create a resource with all possible metadata in it
    resource = await create_resource(storage, maindb_driver, knowledgebox_with_vectorsets)

    async with maindb_driver.ro_transaction() as txn:
        resource.txn = txn  # I don't like this but this is the API we have...
        index_message = await get_resource_index_message(resource, reindex=False)

        # There is a label in the generated resource
        assert "/l/labelset1/label1" in index_message.texts["a/title"].labels

        # Cancel the label and regenerate brain
        assert resource.basic
        resource.basic.usermetadata.ClearField("classifications")
        resource.basic.usermetadata.classifications.add(
            labelset="labelset1", label="label1", cancelled_by_user=True
        )
        index_message = await get_resource_index_message(resource, reindex=False)

        # Label is not generated anymore
        assert "/l/labelset1/label1" not in index_message.texts["a/title"].labels
