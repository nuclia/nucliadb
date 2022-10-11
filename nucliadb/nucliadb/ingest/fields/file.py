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
from typing import Any, Optional

from nucliadb_protos.resources_pb2 import CloudFile, FieldFile, FileExtractedData

from nucliadb.ingest.fields.base import Field
from nucliadb_utils.storages.storage import StorageField

FILE_METADATA = "file_metadata"


class File(Field):
    pbklass = FieldFile
    value: FieldFile
    type: str = "f"
    file_extracted_data: Optional[FileExtractedData]

    def __init__(
        self,
        id: str,
        resource: Any,
        pb: Optional[Any] = None,
        value: Optional[str] = None,
    ):
        super(File, self).__init__(id, resource, pb, value)
        self.file_extracted_data = None

    async def set_value(self, payload: FieldFile):
        old_file = await self.get_value()
        if old_file is None:
            old_cf: Optional[CloudFile] = None
        else:
            old_cf = old_file.file

        is_external_file = payload.file.source == CloudFile.Source.EXTERNAL
        if not is_external_file:
            sf: StorageField = self.storage.file_field(
                self.kbid, self.uuid, self.id, old_cf
            )
            cf: CloudFile = await self.storage.normalize_binary(payload.file, sf)
            payload.file.CopyFrom(cf)

        await self.db_set_value(payload)

    async def get_value(self) -> FieldFile:
        return await self.db_get_value()

    async def set_file_extracted_data(self, file_extracted_data: FileExtractedData):
        if file_extracted_data.HasField("file_preview"):
            sf_file_preview: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, "file_preview"
            )
            cf_file_preview: CloudFile = await self.storage.normalize_binary(
                file_extracted_data.file_preview, sf_file_preview
            )
            file_extracted_data.file_preview.CopyFrom(cf_file_preview)

        for page, preview in enumerate(file_extracted_data.file_pages_previews.pages):
            sf_file_page_preview: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, f"file_pages_previews/{page}"
            )
            cf_file_page_preview: CloudFile = await self.storage.normalize_binary(
                preview, sf_file_page_preview
            )
            file_extracted_data.file_pages_previews.pages[page].CopyFrom(
                cf_file_page_preview
            )

        for fileid, origincf in file_extracted_data.file_generated.items():
            sf_generated: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, f"generated/{fileid}"
            )
            cf_generated: CloudFile = await self.storage.normalize_binary(
                origincf, sf_generated
            )
            file_extracted_data.file_generated[fileid].CopyFrom(cf_generated)

        if file_extracted_data.HasField("file_thumbnail"):
            sf_file_thumbnail: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, "file_thumbnail"
            )
            cf_file_thumbnail: CloudFile = await self.storage.normalize_binary(
                file_extracted_data.file_thumbnail, sf_file_thumbnail
            )
            file_extracted_data.file_thumbnail.CopyFrom(cf_file_thumbnail)

        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FILE_METADATA
        )
        await self.storage.upload_pb(sf, file_extracted_data)
        self.file_extracted_data = file_extracted_data

    async def get_file_extracted_data(self) -> Optional[FileExtractedData]:
        if self.file_extracted_data is None:
            sf: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, FILE_METADATA
            )
            self.file_extracted_data = await self.storage.download_pb(
                sf, FileExtractedData
            )
        return self.file_extracted_data

    async def get_file_extracted_data_cf(self) -> Optional[CloudFile]:
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FILE_METADATA
        )
        if await sf.exists() is not None:
            return sf.build_cf()
        else:
            return None
