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
import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import (
    Classification,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
    Position,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import broker_resource, inject_message


def get_test_broker_message(kbid):
    """
    This simulates a broker messages coming from processing
    where an entity COUNTRY/Spain has been detected for a the field.

    Also, the broker message has some labels at every
    level (resource, field and paragraph).
    """
    bm = broker_resource(kbid=kbid)
    field_id = "text"
    # Add a couple of paragraphs to a text field
    p1 = "My name is Rusty and I am a rust software engineer based in Spain"
    p2 = "I think Python is cool too"
    text = "\n".join([p1, p2])

    field = FieldID()
    field.field = field_id
    field.field_type = FieldType.TEXT

    bm.texts[field_id].body = text

    # Add extracted text
    etw = ExtractedTextWrapper()
    etw.body.text = text
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    # Add field computed metadata with the detected entity
    fmw = FieldComputedMetadataWrapper()
    fmw.field.CopyFrom(field)
    entity = "COUNTRY/Spain"
    family, ent = entity.split("/")
    fmw.metadata.metadata.ner[ent] = family
    pos = Position(start=60, end=64)
    fmw.metadata.metadata.positions[entity].position.append(pos)
    # Add a classification label
    c1 = Classification()
    c1.labelset = "Foo"
    c1.label = "bar"
    fmw.metadata.metadata.classifications.append(c1)

    par1 = Paragraph()
    par1.start = 0
    par1.end = len(p1)
    par1.text = p1
    par2 = Paragraph()
    par2.start = par1.end + 1
    par2.end = par2.start + len(p2)
    par2.text = p2
    fmw.metadata.metadata.paragraphs.append(par1)
    fmw.metadata.metadata.paragraphs.append(par2)
    bm.field_metadata.append(fmw)

    # Add extracted vectors for the field
    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(field)
    p1vec = Vector()
    p1vec.start = par1.start
    p1vec.end = par1.end
    p1vec.vector.extend([1, 1, 1])
    p2vec = Vector()
    p2vec.start = par2.start
    p2vec.end = par2.end
    p2vec.vector.extend([2, 2, 2])
    evw.vectors.vectors.vectors.append(p1vec)
    evw.vectors.vectors.vectors.append(p2vec)
    bm.field_vectors.append(evw)
    return bm


@pytest.mark.asyncio
async def test_find_with_entity_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    bm = get_test_broker_message(knowledgebox)
    await inject_message(nucliadb_grpc, bm)

    # Find + entity filter should return paragraphs of that
    # field even if the paragraphs are not labeled with the entity individually.
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json=dict(
            query="",
            filters=["/e/COUNTRY/Spain"],
            features=["vector"],
            vector=[0.5, 0.5, 0.5],
            min_score=-1,
        ),
    )
    assert resp.status_code == 200
    content = resp.json()
    paragraphs = content["resources"][bm.uuid]["fields"]["/t/text"]["paragraphs"]
    assert len(paragraphs) == 2
