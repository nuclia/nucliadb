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
from nucliadb_protos.resources_pb2 import CloudFile, FieldLayout

from nucliadb.ingest.fields.base import Field
from nucliadb_utils.storages.storage import StorageField


class NotTheSameFormat(Exception):
    pass


class Layout(Field):
    pbklass = FieldLayout
    value: FieldLayout
    type: str = "l"

    async def set_value(self, payload: FieldLayout):
        # Diff support
        actual_payload = await self.get_value()
        if actual_payload and payload.format != actual_payload.format:
            raise NotTheSameFormat()
        if actual_payload is None:
            actual_payload = FieldLayout()
            actual_payload.format = payload.format
        for block in payload.body.deleted_blocks:
            if block in actual_payload.body.blocks:
                del actual_payload.body.blocks[block]

        for ident, pbblock in payload.body.blocks.items():
            if self.storage.needs_move(pbblock.file, self.kbid):
                sf: StorageField = self.storage.layout_field(
                    self.kbid, self.uuid, self.id, ident
                )
                cf: CloudFile = await self.storage.normalize_binary(pbblock.file, sf)
                pbblock.file.CopyFrom(cf)
            actual_payload.body.blocks[ident].CopyFrom(pbblock)
        await self.db_set_value(actual_payload)

    async def get_value(self) -> FieldLayout:
        return await self.db_get_value()
