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

from typing import AsyncIterator

from nucliadb.export_import.context import KBExporterContext
from nucliadb.export_import.models import ExportedItemType


async def export_kb(context: KBExporterContext, kbid: str) -> AsyncIterator[bytes]:
    async for bm in context.data_manager.iter_broker_messages(kbid):
        # Stream the binary files of the broker message
        for cloud_file in context.data_manager.get_binaries(bm):
            binary_size = cloud_file.size
            serialized_cf = cloud_file.SerializeToString()

            yield ExportedItemType.BINARY.encode("utf-8")
            yield len(serialized_cf).to_bytes(4, byteorder="big")
            yield serialized_cf

            yield binary_size.to_bytes(4, byteorder="big")
            async for chunk in context.data_manager.download_binary(cloud_file):
                yield chunk

        # Stream the broker message
        bm_bytes = bm.SerializeToString()
        yield ExportedItemType.RESOURCE.encode("utf-8")
        yield len(bm_bytes).to_bytes(4, byteorder="big")
        yield bm_bytes

        # Stream the entities and labels of the knowledgebox
        entities = await context.data_manager.get_entities(kbid)
        bytes = entities.SerializeToString()
        yield ExportedItemType.ENTITIES.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes

        labels = await context.data_manager.get_labels(kbid)
        bytes = labels.SerializeToString()
        yield ExportedItemType.LABELS.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes
