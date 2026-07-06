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
from os.path import dirname, getsize
from uuid import uuid4

from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.resources_pb2 import CloudFile, FieldType
from nucliadb_protos.resources_pb2 import FieldFile as PBFieldFile
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_field_file(
    local_files, storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None
    filename = f"{dirname(__file__)}/assets/file.png"

    cf1 = CloudFile(
        uri="file.png",
        source=CloudFile.Source.LOCAL,
        bucket_name="/integration/orm/assets",
        size=getsize(filename),
        content_type="image/png",
        filename="file.png",
    )

    t2 = PBFieldFile(
        language="es",
    )
    t2.added.FromDatetime(datetime.now())
    t2.file.CopyFrom(cf1)

    await r.set_field(FieldType.FILE, "file1", t2)

    filefield: File = await r.get_field("file1", FieldType.FILE, load=True)
    assert filefield.value.file == t2.file

    assert filefield.value.file.source == storage.source
    data = await storage.downloadbytescf(filefield.value.file)
    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    assert data.read() == data2
