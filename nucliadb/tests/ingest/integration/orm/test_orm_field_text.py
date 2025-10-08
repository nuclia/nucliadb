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
from uuid import uuid4

from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.resources_pb2 import FieldText as PBFieldText
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_field_text(
    storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    t2 = PBFieldText(body="This is my text field", format=PBFieldText.Format.PLAIN)
    await r.set_field(FieldType.TEXT, "text1", t2)

    textfield: Text = await r.get_field("text1", FieldType.TEXT, load=True)
    assert textfield.value.body == t2.body


async def test_create_resource_orm_field_text_file(
    storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    t2 = PBFieldText(body="This is my text field", format=PBFieldText.Format.PLAIN)

    await r.set_field(FieldType.TEXT, "text1", t2)

    textfield: Text = await r.get_field("text1", FieldType.TEXT, load=True)
    assert textfield.value.body == t2.body
