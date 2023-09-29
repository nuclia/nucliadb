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
from nucliadb_protos import utils_pb2


@pytest.mark.asyncio
async def test_create_resource_orm_with_basic(
    gcs_storage, txn, cache, fake_node, knowledgebox_ingest: str
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
    kb_obj = KnowledgeBox(txn, gcs_storage, kbid=knowledgebox_ingest)
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
    o2.source_id = "My Surce"
    o2.created.FromDatetime(datetime.now())

    await r.set_origin(o2)
    o2 = await r.get_origin()
    assert o2 is not None
    assert o2.source_id == "My Surce"


@pytest.mark.asyncio
async def test_iterate_paragraphs(
    gcs_storage, txn, cache, fake_node, knowledgebox_ingest: str
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
    kb_obj = KnowledgeBox(txn, gcs_storage, kbid=knowledgebox_ingest)
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
    gcs_storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    basic = PBBasic(title="My title", summary="My summary")
    basic.metadata.status = PBMetadata.Status.PROCESSED

    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, gcs_storage, kbid=knowledgebox_ingest)
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


async def test_generate_field_conversation(
    gcs_storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    # Create a resource with a conversation field (with multiple pages of messages)

    # Check that generate field copies over the right data from the conversation
    pass
