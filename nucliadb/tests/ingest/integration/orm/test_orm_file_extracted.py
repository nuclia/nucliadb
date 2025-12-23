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

from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    FieldType,
    FileExtractedData,
    RowsPreview,
)
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_file_extracted(
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

    ex1 = FileExtractedData()
    ex1.md5 = "ASD"
    ex1.language = "ca"
    ex1.metadata["asd"] = "asd"
    ex1.nested["asd"] = "asd"
    ex1.file_generated["asd"].CopyFrom(cf1)
    ex1.file_rows_previews["asd"].sheets["Tab1"].rows.append(RowsPreview.Sheet.Row(cell="hola"))
    ex1.file_preview.CopyFrom(cf1)
    ex1.file_thumbnail.CopyFrom(cf1)
    ex1.file_pages_previews.pages.append(cf1)
    ex1.field = "file1"

    field_obj: File = await r.get_field(ex1.field, FieldType.FILE, load=False)
    await field_obj.set_file_extracted_data(ex1)

    ex2: FileExtractedData | None = await field_obj.get_file_extracted_data()
    assert ex2 is not None
    assert ex2.md5 == ex1.md5
    assert ex2.file_generated["asd"].source == storage.source
    assert ex2.file_preview.source == storage.source
    assert ex2.file_thumbnail.source == storage.source
    assert ex1.file_pages_previews.pages[0].source == storage.source
    data = await storage.downloadbytescf(ex1.file_pages_previews.pages[0])
    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    assert data.read() == data2
