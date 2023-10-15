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
from unittest.mock import AsyncMock, Mock

from nucliadb.export_import.datamanager import (
    ExportImportDataManager,
    iter_and_add_size,
    iter_in_chunk_size,
)
from nucliadb_protos import resources_pb2


async def testiter_and_add_size():
    cf = resources_pb2.CloudFile()

    async def iter():
        yield b"foo"
        yield b"bar"

    cf.size = 0
    async for _ in iter_and_add_size(iter(), cf):
        pass

    assert cf.size == 6


async def test_iter_in_chunk_size():
    async def iterable(n):
        for i in range(n):
            yield str(i).encode()

    chunks = [chunk async for chunk in iter_in_chunk_size(iterable(10), chunk_size=4)]
    assert len(chunks[0]) == 4
    assert len(chunks[1]) == 4
    assert len(chunks[2]) == 2

    chunks = [chunk async for chunk in iter_in_chunk_size(iterable(0), chunk_size=4)]
    assert len(chunks) == 0


async def test_try_delete_from_storage():
    driver = Mock()
    storage = Mock()

    dm = ExportImportDataManager(driver, storage)
    dm.delete_export = AsyncMock(side_effect=ValueError())  # type: ignore
    dm.delete_import = AsyncMock(side_effect=ValueError())  # type: ignore

    await dm.try_delete_from_storage("export", "kbid", "foo")
    await dm.try_delete_from_storage("import", "kbid", "foo")

    dm.delete_export.assert_called()
    dm.delete_import.assert_called()
