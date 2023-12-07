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

import pytest
from nucliadb_protos.resources_pb2 import Block, CloudFile
from nucliadb_protos.resources_pb2 import FieldLayout as PBFieldLayout
from nucliadb_protos.resources_pb2 import FieldType

from nucliadb.ingest.fields.layout import Layout
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_utils.storages.storage import Storage


@pytest.mark.asyncio
async def test_create_resource_orm_field_layout(
    storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    l2 = PBFieldLayout(format=PBFieldLayout.Format.NUCLIAv1)
    l2.body.blocks["field1"].x = 0
    l2.body.blocks["field1"].y = 0
    l2.body.blocks["field1"].cols = 1
    l2.body.blocks["field1"].rows = 1
    l2.body.blocks["field1"].type = Block.TypeBlock.TITLE
    l2.body.blocks["field1"].payload = "{}"

    await r.set_field(FieldType.LAYOUT, "layout1", l2)

    layoutfield: Layout = await r.get_field("layout1", FieldType.LAYOUT, load=True)
    assert layoutfield.value.body == l2.body

    l2 = PBFieldLayout(format=PBFieldLayout.Format.NUCLIAv1)
    l2.body.blocks["ident2"].x = 0
    l2.body.blocks["ident2"].y = 2
    l2.body.blocks["ident2"].cols = 1
    l2.body.blocks["ident2"].rows = 1
    l2.body.blocks["ident2"].type = Block.TypeBlock.TITLE
    l2.body.blocks["ident2"].payload = "{}"

    await r.set_field(FieldType.LAYOUT, "layout1", l2)
    layoutfield = await r.get_field("layout1", FieldType.LAYOUT, load=True)
    assert len(layoutfield.value.body.blocks.keys()) == 2

    l2 = PBFieldLayout(format=PBFieldLayout.Format.NUCLIAv1)
    l2.body.deleted_blocks.append("ident2")
    await r.set_field(FieldType.LAYOUT, "layout1", l2)
    layoutfield = await r.get_field("layout1", FieldType.LAYOUT, load=True)
    assert len(layoutfield.value.body.blocks.keys()) == 1


@pytest.mark.asyncio
async def test_create_resource_orm_field_layout_file(
    local_files, storage: Storage, txn, cache, fake_node, knowledgebox_ingest: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    l2 = PBFieldLayout(format=PBFieldLayout.Format.NUCLIAv1)
    l2.body.blocks["field1"].x = 0
    l2.body.blocks["field1"].y = 0
    l2.body.blocks["field1"].cols = 1
    l2.body.blocks["field1"].rows = 1
    l2.body.blocks["field1"].type = Block.TypeBlock.TITLE
    l2.body.blocks["field1"].ident = "ident1"
    l2.body.blocks["field1"].payload = "{}"

    filename = f"{dirname(__file__)}/assets/file.png"

    cf1 = CloudFile(
        uri="file.png",
        source=CloudFile.Source.LOCAL,
        bucket_name="/integration/orm/assets",
        size=getsize(filename),
        content_type="image/png",
        filename="file.png",
    )
    l2.body.blocks["field1"].file.CopyFrom(cf1)

    await r.set_field(FieldType.LAYOUT, "layout1", l2)

    layoutfield: Layout = await r.get_field("layout1", FieldType.LAYOUT, load=True)
    assert layoutfield.value.body == l2.body

    assert layoutfield.value.body.blocks["field1"].file.source == CloudFile.Source.GCS
    data = await storage.downloadbytescf(layoutfield.value.body.blocks["field1"].file)
    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    assert data.read() == data2
