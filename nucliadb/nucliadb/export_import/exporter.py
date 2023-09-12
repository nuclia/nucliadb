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

import tempfile
from typing import AsyncIterator
from uuid import uuid4

import aiofiles

from nucliadb.export_import.context import KBExporterContext
from nucliadb.export_import.models import CODEX

BINARY_CHUNK_SIZE = 1024 * 1024


async def iter_file_chunks(filename: str) -> AsyncIterator[bytes]:
    async with aiofiles.open(filename, mode="rb") as f:
        while True:
            chunk = await f.read(BINARY_CHUNK_SIZE)
            if not chunk:
                break
            yield chunk


async def export_kb(context: KBExporterContext, kbid: str) -> AsyncIterator[bytes]:
    with tempfile.TemporaryDirectory() as dest_dir:
        async for bm in context.data_manager.iter_broker_messages(kbid):
            # Stream the binary files of the broker message
            binary_aux_file = f"{dest_dir}/{uuid4().hex}"
            for cloud_file in context.data_manager.get_binaries(bm):
                async with aiofiles.open(binary_aux_file, "wb") as binary_file:
                    file_size = await context.data_manager.download_binary(
                        cloud_file, binary_file
                    )
                serialized_cf = cloud_file.SerializeToString()
                yield CODEX.BINARY.encode("utf-8")
                yield len(serialized_cf).to_bytes(4, byteorder="big")
                yield serialized_cf
                yield file_size.to_bytes(4, byteorder="big")
                async for file_chunk in iter_file_chunks(binary_aux_file):
                    yield file_chunk

            # Stream the broker message
            bm_bytes = bm.SerializeToString()
            yield CODEX.RESOURCE.encode("utf-8")
            yield len(bm_bytes).to_bytes(4, byteorder="big")
            yield bm_bytes

        # Stream the entities and labels of the knowledgebox
        entities = await context.data_manager.get_entities(kbid)
        bytes = entities.SerializeToString()
        yield CODEX.ENTITIES.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes

        labels = await context.data_manager.get_labels(kbid)
        bytes = labels.SerializeToString()
        yield CODEX.LABELS.encode("utf-8")
        yield len(bytes).to_bytes(4, byteorder="big")
        yield bytes
