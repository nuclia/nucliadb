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
from typing import Annotated, AsyncGenerator

from fastapi import Depends
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.export_import.codecs import CODEX
from nucliadb.export_import.context import (
    ExporterContext,
    get_exporter_context_from_app,
)
from nucliadb.export_import.importer import ExportItem, import_kb
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


class StreamReader:
    def __init__(self, stream):
        self.stream = stream
        self.gen = None
        self.buffer = b""

    def _read_from_buffer(self, n_bytes: int):
        value = self.buffer[:n_bytes]
        self.buffer = self.buffer[n_bytes:]
        return value

    async def read(self, n_bytes: int):
        if self.gen is None:
            self.gen = self.stream.__aiter__()

        while True:
            try:
                if self.buffer != b"" and len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)

                self.buffer += await self.gen.__anext__()
                if len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)

            except StopAsyncIteration:
                if self.buffer != b"":
                    return self._read_from_buffer(n_bytes)
                else:
                    raise


class ImportStreamReader(StreamReader):
    async def iter_to_import(self) -> AsyncGenerator[ExportItem]:
        while True:
            try:
                codex_bytes = await self.read(3)
                codex = CODEX(codex_bytes.decode())
                size_bytes = await self.read(4)
                size = int.from_bytes(size_bytes, byteorder="big")
                data = await self.read(size)
                yield codex, data
            except StopAsyncIteration:
                break


async def contexts(request: Request) -> ExporterContext:
    return {"importer": get_exporter_context_from_app(request.app)}


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/import",
    status_code=200,
    name="Import a Knowledge Box",
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.READER])
@version(1)
async def import_kb_endpoint(
    request: Request, kbid: str, contexts: Annotated[dict, Depends(contexts)]
):
    stream_reader = ImportStreamReader(request.stream())
    await import_kb(
        context=contexts["importer"],
        kbid=kbid,
        generator=stream_reader.iter_to_import(),
    )
