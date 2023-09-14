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

from typing import AsyncGenerator

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.models import ExportedItemType
from nucliadb.export_import.utils import (
    download_binary,
    get_cloud_files,
    get_entities,
    get_labels,
    iter_broker_messages,
)


async def export_kb(
    context: ApplicationContext, kbid: str
) -> AsyncGenerator[bytes, None]:
    """Export the data of a knowledgebox to a stream of bytes.

    See https://github.com/nuclia/nucliadb/blob/main/docs/internal/EXPORTS.md
    for an overview on format of the export file/stream.
    """
    async for bm in iter_broker_messages(context, kbid):
        # Stream the binary files of the broker message
        for cloud_file in get_cloud_files(bm):
            binary_size = cloud_file.size
            serialized_cf = cloud_file.SerializeToString()

            yield ExportedItemType.BINARY.encode("utf-8")
            yield len(serialized_cf).to_bytes(4, byteorder="big")
            yield serialized_cf

            yield binary_size.to_bytes(4, byteorder="big")
            async for chunk in download_binary(context, cloud_file):
                yield chunk

        # Stream the broker message
        bm_bytes = bm.SerializeToString()
        yield ExportedItemType.RESOURCE.encode("utf-8")
        yield len(bm_bytes).to_bytes(4, byteorder="big")
        yield bm_bytes

    # Stream the entities and labels of the knowledgebox
    entities = await get_entities(context, kbid)
    data = entities.SerializeToString()
    yield ExportedItemType.ENTITIES.encode("utf-8")
    yield len(data).to_bytes(4, byteorder="big")
    yield data

    labels = await get_labels(context, kbid)
    data = labels.SerializeToString()
    yield ExportedItemType.LABELS.encode("utf-8")
    yield len(data).to_bytes(4, byteorder="big")
    yield data
