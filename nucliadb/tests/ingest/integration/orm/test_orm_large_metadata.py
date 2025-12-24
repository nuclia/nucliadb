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
from uuid import uuid4

from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    Entity,
    FieldID,
    FieldType,
    LargeComputedMetadata,
    LargeComputedMetadataWrapper,
)
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_large_metadata(
    storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = LargeComputedMetadataWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))
    en1 = Entity(token="tok1", root="tok", type="NAME")
    en2 = Entity(token="tok2", root="tok2", type="NAME")
    ex1.real.metadata.entities.append(en1)
    ex1.real.metadata.entities.append(en2)
    ex1.real.metadata.tokens["tok"] = 3

    field_obj: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_large_field_metadata(ex1)

    ex2: LargeComputedMetadata | None = await field_obj.get_large_field_metadata()
    assert ex2 is not None
    assert ex2.metadata.tokens["tok"] == ex1.real.metadata.tokens["tok"]


async def test_create_resource_orm_large_metadata_file(
    local_files, storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = LargeComputedMetadataWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))

    en1 = Entity(token="tok1", root="tok", type="NAME")
    en2 = Entity(token="tok2", root="tok2", type="NAME")
    real = LargeComputedMetadata()
    real.metadata.entities.append(en1)
    real.metadata.entities.append(en2)
    real.metadata.tokens["tok"] = 3
    real.metadata.tokens["adeu"] = 5

    filename = f"{dirname(__file__)}/assets/largemetadata.pb"
    with open(filename, "wb") as testfile:
        testfile.write(real.SerializeToString())
    cf1 = CloudFile(
        uri="largemetadata.pb",
        source=CloudFile.Source.LOCAL,
        bucket_name="/integration/orm/assets",
        size=getsize(filename),
        content_type="application/octet-stream",
        filename="largemetadata.pb",
    )
    ex1.file.CopyFrom(cf1)

    field_obj: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_large_field_metadata(ex1)

    ex2: LargeComputedMetadata | None = await field_obj.get_large_field_metadata()
    assert ex2 is not None
    ex3 = LargeComputedMetadata()
    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    ex3.ParseFromString(data2)
    assert ex3.metadata.tokens["tok"] == ex2.metadata.tokens["tok"]
