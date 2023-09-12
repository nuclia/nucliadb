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
import asyncio

from fastapi import FastAPI
from starlette.requests import Request

from nucliadb.export_import.context import KBExporterContext, KBImporterContext
from nucliadb.export_import.importer import ExportStream


class FastAPIExportStream(ExportStream):
    """
    Adapts a FastAPI request stream to the ExportStream interface so that we
    can import from a export binary being streamed.
    """

    def __init__(self, request: Request):
        self.stream = request.stream().__aiter__()
        self.buffer = b""

    def _read_from_buffer(self, n_bytes: int) -> bytes:
        value = self.buffer[:n_bytes]
        self.buffer = self.buffer[n_bytes:]
        return value

    async def read(self, n_bytes: int) -> bytes:
        while True:
            try:
                if self.buffer != b"" and len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)

                self.buffer += await self.stream.__anext__()
                if len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)

            except StopAsyncIteration:
                if self.buffer != b"":
                    return self._read_from_buffer(n_bytes)
                else:
                    raise


def set_exporter_context(app: FastAPI):
    context = KBExporterContext(service_name="kb_exporter")
    asyncio.run(context.initialize())
    app.state.exporter_context = context
    app.add_event_handler("shutdown", context.finalize)


def get_exporter_context(app: FastAPI) -> KBExporterContext:
    return app.state.exporter_context


def set_importer_context(app: FastAPI):
    context = KBImporterContext(service_name="kb_imorter")
    asyncio.run(context.initialize())
    app.state.importer_context = context
    app.add_event_handler("shutdown", context.finalize)


def get_importer_context(app: FastAPI) -> KBImporterContext:
    return app.state.importer_context
