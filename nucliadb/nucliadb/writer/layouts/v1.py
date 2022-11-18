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
from nucliadb_protos.resources_pb2 import Block as PBBlock
from nucliadb_protos.resources_pb2 import FieldLayout

import nucliadb_models as models
from nucliadb.writer.layouts import VERSION
from nucliadb_utils.storages.storage import Storage


async def serialize_block(
    layout_field: models.InputLayoutField,
    kbid: str,
    uuid: str,
    field: str,
    storage: Storage,
) -> FieldLayout:
    pblayout = FieldLayout()
    for key, block in layout_field.body.blocks.items():
        pbblock = PBBlock()
        pbblock.x = block.x
        pbblock.y = block.y
        pbblock.cols = block.cols
        pbblock.rows = block.rows
        pbblock.type = PBBlock.TypeBlock.Value(block.type)
        pbblock.ident = block.ident if block.ident else key
        pbblock.payload = block.payload

        sf = storage.layout_field(kbid, uuid, field, key)
        await storage.upload_b64file_to_cloudfile(
            sf,
            block.file.payload.encode(),
            block.file.filename,
            block.file.content_type,
            block.file.md5,
        )
        pblayout.body.blocks[key].CopyFrom(pbblock)
        pblayout.format = FieldLayout.Format.Value(layout_field.format.value)
    return pblayout


VERSION[models.LayoutFormat.NUCLIAv1] = serialize_block
