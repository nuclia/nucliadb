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

from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedText,
    ExtractedTextWrapper,
    FieldID,
    FieldType,
)
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_extracted(
    storage: Storage, txn, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = ExtractedTextWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))
    ex1.body.text = "My Text"

    field_obj: Optional[Text] = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    assert field_obj is not None
    await field_obj.set_extracted_text(ex1)

    ex2: Optional[ExtractedText] = await field_obj.get_extracted_text()
    assert ex2 is not None
    assert ex2.text == ex1.body.text


async def test_create_resource_orm_extracted_file(
    local_files,
    storage: Storage,
    txn,
    dummy_nidx_utility,
    knowledgebox: str,
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    ex1 = ExtractedTextWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))

    filename = f"{dirname(__file__)}/assets/text.pb"
    cf1 = CloudFile(
        uri="text.pb",
        source=CloudFile.Source.LOCAL,
        bucket_name="/integration/orm/assets",
        size=getsize(filename),
        content_type="application/octet-stream",
        filename="text.pb",
    )
    ex1.file.CopyFrom(cf1)

    field_obj: Optional[Text] = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    assert field_obj is not None
    await field_obj.set_extracted_text(ex1)

    ex2: Optional[ExtractedText] = await field_obj.get_extracted_text()
    assert ex2 is not None
    ex3 = ExtractedText()
    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    ex3.ParseFromString(data2)
    assert ex3.text == ex2.text


async def test_create_resource_orm_extracted_delta(
    storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None
    ex1 = ExtractedTextWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.CONVERSATION, field="text1"))
    ex1.body.split_text["ident1"] = "My text"
    ex1.body.text = "all text"

    field_obj: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_extracted_text(ex1)

    ex2: Optional[ExtractedText] = await field_obj.get_extracted_text()
    assert ex2 is not None
    assert ex2.text == ex1.body.text

    ex1 = ExtractedTextWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.CONVERSATION, field="text1"))
    ex1.body.split_text["ident2"] = "My text"
    ex1.body.text = "all text 2"

    field_obj = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_extracted_text(ex1)

    ex2 = await field_obj.get_extracted_text()
    assert ex2 is not None
    assert ex2.text == ex1.body.text
    assert len(ex2.split_text) == 2
