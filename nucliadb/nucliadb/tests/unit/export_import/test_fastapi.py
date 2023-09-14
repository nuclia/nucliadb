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
from io import BytesIO

import pytest
from starlette.requests import Request

from nucliadb.export_import.exceptions import ExportStreamExhausted
from nucliadb.export_import.fastapi import FastAPIExportStream


class TestRequest(Request):
    def __init__(self, data: bytes, receive_chunk_size: int = 10):
        super().__init__(
            scope={
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "headers": [],
            },
            receive=self.receive,
        )
        self.receive_chunk_size = receive_chunk_size
        self.bytes = BytesIO(data)

    async def receive(self):
        chunk = self.bytes.read(self.receive_chunk_size)
        more_data = True
        if chunk == b"":
            more_data = False
        return {"type": "http.request", "body": chunk, "more_body": more_data}


async def test_export_stream():
    request = TestRequest(data=b"01234XYZ", receive_chunk_size=2)

    export_stream = FastAPIExportStream(request)
    assert await export_stream.read(0) == b""

    for i in range(5):
        assert await export_stream.read(1) == f"{i}".encode()

    assert await export_stream.read(3) == b"XYZ"

    with pytest.raises(ExportStreamExhausted):
        await export_stream.read(1)

    with pytest.raises(ExportStreamExhausted):
        await export_stream.read(0)

    request = TestRequest(data=b"foobar", receive_chunk_size=2)
    export_stream = FastAPIExportStream(request)
    assert await export_stream.read(50) == b"foobar"
    with pytest.raises(ExportStreamExhausted):
        await export_stream.read(0)
