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

from nucliadb.export_import.exceptions import ExportStreamExhausted
from nucliadb.export_import.importer import ExportStream


async def test_export_stream():
    export = BytesIO(b"1234567890")
    stream = ExportStream(export)
    assert stream.read_bytes == 0
    assert stream.length == 10
    assert await stream.read(0) == b""
    assert stream.read_bytes == 0
    assert await stream.read(1) == b"1"
    assert stream.read_bytes == 1
    assert await stream.read(2) == b"23"
    assert stream.read_bytes == 3
    assert await stream.read(50) == b"4567890"
    assert stream.read_bytes == 10
    with pytest.raises(ExportStreamExhausted):
        await stream.read(1)
