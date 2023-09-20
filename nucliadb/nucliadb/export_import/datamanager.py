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

from typing import Optional

from nucliadb.common.maindb.driver import Driver
from nucliadb.export_import.exceptions import MetadataNotFound
from nucliadb.export_import.models import ExportMetadata, ImportMetadata

KB_EXPORTS = "/kbs/{kbid}/exports"
KB_IMPORTS = "/kbs/{kbid}/imports"

KB_EXPORT = KB_EXPORTS + "/{export_id}"
KB_IMPORT = KB_IMPORTS + "/{import_id}"


class ExportImportDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def get_export_metadata(self, kbid: str, export_id: str) -> ExportMetadata:
        key = KB_EXPORT.format(kbid=kbid, export_id=export_id)
        data = await self._get(key)
        if data is None or data == b"":
            raise MetadataNotFound()
        decoded = data.decode("utf-8")
        return ExportMetadata.parse_raw(decoded)

    async def set_export_metadata(
        self, kbid: str, export_id: str, metadata: ExportMetadata
    ):
        key = KB_EXPORT.format(kbid=kbid, export_id=export_id)
        data = metadata.json().encode("utf-8")
        await self._set(key, data)

    async def get_import_metadata(self, kbid: str, import_id: str) -> ImportMetadata:
        key = KB_IMPORT.format(kbid=kbid, import_id=import_id)
        data = await self._get(key)
        if data is None or data == b"":
            raise MetadataNotFound()
        decoded = data.decode("utf-8")
        return ImportMetadata.parse_raw(decoded)

    async def set_import_metadata(
        self, kbid: str, import_id: str, metadata: ImportMetadata
    ):
        key = KB_IMPORT.format(kbid=kbid, import_id=import_id)
        data = metadata.json().encode("utf-8")
        await self._set(key, data)

    async def _get(self, key: str) -> Optional[bytes]:
        async with self.driver.transaction() as txn:
            return await txn.get(key)

    async def _set(self, key: str, data: bytes):
        async with self.driver.transaction() as txn:
            await txn.set(key, data)
            await txn.commit()
