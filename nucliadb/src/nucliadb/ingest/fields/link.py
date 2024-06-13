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

from nucliadb.ingest.fields.base import Field
from nucliadb_protos.resources_pb2 import CloudFile, FieldLink, LinkExtractedData
from nucliadb_utils.storages.storage import StorageField

LINK_METADATA = "link_metadata"


class Link(Field):
    pbklass = FieldLink
    value: FieldLink
    type: str = "u"
    link_extracted_data: Optional[LinkExtractedData]

    def __init__(
        self,
        id: str,
        resource: Any,
        pb: Optional[Any] = None,
        value: Optional[str] = None,
    ):
        super(Link, self).__init__(id, resource, pb, value)
        self.link_extracted_data = None

    async def set_value(self, payload: FieldLink):
        await self.db_set_value(payload)

    async def get_value(self) -> FieldLink:
        return await self.db_get_value()

    async def set_link_extracted_data(self, link_extracted_data: LinkExtractedData):
        if link_extracted_data.HasField("link_thumbnail"):
            sf_link_thumbnail: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, "link_thumbnail"
            )
            cf_link_thumbnail: CloudFile = await self.storage.normalize_binary(
                link_extracted_data.link_thumbnail, sf_link_thumbnail
            )
            link_extracted_data.link_thumbnail.CopyFrom(cf_link_thumbnail)
        if link_extracted_data.HasField("link_preview"):
            sf_link_preview: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, "link_preview"
            )
            cf_link_preview: CloudFile = await self.storage.normalize_binary(
                link_extracted_data.link_preview, sf_link_preview
            )
            link_extracted_data.link_preview.CopyFrom(cf_link_preview)

        if link_extracted_data.HasField("link_image"):
            sf_link_image: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, "link_image"
            )
            cf_link_image: CloudFile = await self.storage.normalize_binary(
                link_extracted_data.link_image, sf_link_image
            )
            link_extracted_data.link_image.CopyFrom(cf_link_image)

        for fileid, origincf in link_extracted_data.file_generated.items():
            sf_generated: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, f"generated/{fileid}"
            )
            cf_generated: CloudFile = await self.storage.normalize_binary(origincf, sf_generated)
            link_extracted_data.file_generated[fileid].CopyFrom(cf_generated)

        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, LINK_METADATA
        )
        await self.storage.upload_pb(sf, link_extracted_data)
        self.link_extracted_data = link_extracted_data

    async def get_link_extracted_data(self) -> Optional[LinkExtractedData]:
        if self.link_extracted_data is None:
            sf: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, LINK_METADATA
            )
            self.link_extracted_data = await self.storage.download_pb(sf, LinkExtractedData)
        return self.link_extracted_data
