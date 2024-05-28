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
from os.path import dirname, getsize
from typing import Optional
from uuid import uuid4

import pytest
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedVectorsWrapper,
    FieldID,
    FieldType,
)
from nucliadb_protos.utils_pb2 import Vector, VectorObject, Vectors

from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_utils.storages.storage import Storage


@pytest.mark.asyncio
async def test_create_resource_orm_vector(
    storage: Storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))
    v1 = Vector(start=1, end=2, vector=b"ansjkdn")
    ex1.vectors.vectors.vectors.append(v1)

    field_obj: Text = await r.get_field(
        ex1.field.field, ex1.field.field_type, load=False
    )
    await field_obj.set_vectors(ex1)

    ex2: Optional[VectorObject] = await field_obj.get_vectors()
    assert ex2 is not None
    assert ex2.vectors.vectors[0].vector == ex1.vectors.vectors.vectors[0].vector


@pytest.mark.asyncio
async def test_create_resource_orm_vector_file(
    local_files, storage: Storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))

    filename = f"{dirname(__file__)}/assets/vectors.pb"
    cf1 = CloudFile(
        uri="vectors.pb",
        source=CloudFile.Source.LOCAL,
        size=getsize(filename),
        bucket_name="/integration/orm/assets",
        content_type="application/octet-stream",
        filename="vectors.pb",
    )
    ex1.file.CopyFrom(cf1)

    field_obj: Text = await r.get_field(
        ex1.field.field, ex1.field.field_type, load=False
    )
    await field_obj.set_vectors(ex1)

    ex2: Optional[VectorObject] = await field_obj.get_vectors()
    ex3 = VectorObject()
    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    ex3.ParseFromString(data2)
    assert ex2 is not None

    assert ex3.vectors.vectors[0].vector == ex2.vectors.vectors[0].vector


@pytest.mark.asyncio
async def test_create_resource_orm_vector_split(
    storage: Storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.LAYOUT, field="text1"))
    v1 = Vector(start=1, vector=b"ansjkdn")
    vs1 = Vectors()
    vs1.vectors.append(v1)
    ex1.vectors.split_vectors["es1"].vectors.append(v1)
    ex1.vectors.split_vectors["es2"].vectors.append(v1)

    field_obj: Text = await r.get_field(
        ex1.field.field, ex1.field.field_type, load=False
    )
    await field_obj.set_vectors(ex1)

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.LAYOUT, field="text1"))
    v1 = Vector(start=1, vector=b"ansjkdn")
    vs1 = Vectors()
    vs1.vectors.append(v1)
    ex1.vectors.split_vectors["es3"].vectors.append(v1)
    ex1.vectors.split_vectors["es2"].vectors.append(v1)

    field_obj2: Text = await r.get_field(
        ex1.field.field, ex1.field.field_type, load=False
    )
    await field_obj2.set_vectors(ex1)

    ex2: Optional[VectorObject] = await field_obj2.get_vectors()
    assert ex2 is not None
    assert len(ex2.split_vectors) == 3
