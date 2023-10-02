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
from typing import AsyncGenerator, Callable, cast

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import import logger
from nucliadb.export_import.models import ExportedItemType
from nucliadb.export_import.utils import (
    ExportStream,
    ExportStreamReader,
    import_binary,
    import_broker_message,
    set_entities_groups,
    set_labels,
)
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos import resources_pb2, writer_pb2

BinaryStream = AsyncGenerator[bytes, None]
BinaryStreamGenerator = Callable[[int], BinaryStream]


async def import_kb(
    context: ApplicationContext, kbid: str, stream: ExportStream
) -> None:
    """
    Imports exported data from a stream into a knowledgebox.
    """
    stream_reader = ExportStreamReader(stream)
    async for item_type, data in stream_reader.iter_items():
        if item_type == ExportedItemType.RESOURCE:
            bm = cast(writer_pb2.BrokerMessage, data)
            await import_broker_message(context, kbid, bm)

        elif item_type == ExportedItemType.BINARY:
            cf = cast(resources_pb2.CloudFile, data[0])
            binary_generator = cast(BinaryStreamGenerator, data[1])
            await import_binary(context, kbid, cf, binary_generator)

        elif item_type == ExportedItemType.ENTITIES:
            entities = cast(kb_pb2.EntitiesGroups, data)
            await set_entities_groups(context, kbid, entities)

        elif item_type == ExportedItemType.LABELS:
            labels = cast(kb_pb2.Labels, data)
            await set_labels(context, kbid, labels)

        else:
            logger.warning(f"Unknown exporteed item type: {item_type}")
            continue
