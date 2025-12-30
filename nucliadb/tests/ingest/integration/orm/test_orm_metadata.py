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
from uuid import uuid4

from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.resources_pb2 import (
    Classification,
    FieldComputedMetadata,
    FieldComputedMetadataWrapper,
    FieldEntity,
    FieldID,
    FieldType,
    Paragraph,
    Position,
    Sentence,
)
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_metadata(
    storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = FieldComputedMetadataWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))
    ex1.metadata.metadata.links.append("https://nuclia.com")

    p1 = Paragraph(start=0, end=20)
    p1.sentences.append(Sentence(start=0, end=10, key="test"))
    p1.sentences.append(Sentence(start=11, end=20, key="test"))
    cl1 = Classification(labelset="labelset1", label="label1")
    p1.classifications.append(cl1)
    ex1.metadata.metadata.mime_type = "text/plain"
    ex1.metadata.metadata.paragraphs.append(p1)
    ex1.metadata.metadata.classifications.append(cl1)
    ex1.metadata.metadata.last_index.FromDatetime(datetime.now())
    ex1.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    ex1.metadata.metadata.last_extract.FromDatetime(datetime.now())
    # Data Augmentation + Processor entities
    ex1.metadata.metadata.entities["my-task-id"].entities.extend(
        [
            FieldEntity(
                text="Ramon",
                label="PEOPLE",
                positions=[Position(start=0, end=5), Position(start=23, end=28)],
            )
        ]
    )

    field_obj: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_field_metadata(ex1)

    ex2: FieldComputedMetadata | None = await field_obj.get_field_metadata()
    assert ex2 is not None
    assert ex2.metadata.links[0] == ex1.metadata.metadata.links[0]
    assert ex2.metadata.mime_type == ex1.metadata.metadata.mime_type


async def test_create_resource_orm_metadata_split(
    storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = FieldComputedMetadataWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.CONVERSATION, field="text1"))
    ex1.metadata.split_metadata["ff1"].links.append("https://nuclia.com")
    s2 = Sentence(start=0, end=10, key="test")
    s1 = Sentence(start=11, end=20, key="test")

    p1 = Paragraph(start=0, end=20)
    p1.sentences.append(s1)
    p1.sentences.append(s2)
    cl1 = Classification(labelset="labelset1", label="label1")
    p1.classifications.append(cl1)
    ex1.metadata.split_metadata["ff1"].paragraphs.append(p1)
    ex1.metadata.split_metadata["ff1"].classifications.append(cl1)
    ex1.metadata.split_metadata["ff1"].entities["processor"].entities.extend(
        [FieldEntity(text="Ramon", label="PERSON")]
    )

    ex1.metadata.split_metadata["ff1"].last_index.FromDatetime(datetime.now())
    ex1.metadata.split_metadata["ff1"].last_understanding.FromDatetime(datetime.now())
    ex1.metadata.split_metadata["ff1"].last_extract.FromDatetime(datetime.now())
    field_obj: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_field_metadata(ex1)

    ex2 = FieldComputedMetadataWrapper()
    ex2.field.CopyFrom(FieldID(field_type=FieldType.CONVERSATION, field="text1"))
    ex2.metadata.split_metadata["ff2"].links.append("https://nuclia.com")
    s1 = Sentence(start=0, end=10, key="test")
    s2 = Sentence(start=11, end=20, key="test")

    p1 = Paragraph(start=0, end=20)
    p1.sentences.append(s1)
    p1.sentences.append(s2)
    cl1 = Classification(labelset="labelset1", label="label1")
    p1.classifications.append(cl1)
    ex2.metadata.split_metadata["ff2"].paragraphs.append(p1)
    ex2.metadata.split_metadata["ff2"].classifications.append(cl1)
    ex1.metadata.split_metadata["ff1"].entities["processor"].entities.extend(
        [FieldEntity(text="Ramon", label="PEOPLE")]
    )
    ex2.metadata.split_metadata["ff2"].last_index.FromDatetime(datetime.now())
    ex2.metadata.split_metadata["ff2"].last_understanding.FromDatetime(datetime.now())
    ex2.metadata.split_metadata["ff2"].last_extract.FromDatetime(datetime.now())
    field_obj = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_field_metadata(ex2)

    ex3: FieldComputedMetadata | None = await field_obj.get_field_metadata()
    assert ex3 is not None
    assert ex1.metadata.split_metadata["ff1"].links[0] == ex3.split_metadata["ff1"].links[0]
    assert len(ex3.split_metadata) == 2
