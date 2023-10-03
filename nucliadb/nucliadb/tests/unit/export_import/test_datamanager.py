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
from nucliadb.export_import.datamanager import _iter_and_add_size_to_cf
from nucliadb_protos import resources_pb2


async def test_iter_and_add_size_to_cf():
    cf = resources_pb2.CloudFile()

    async def iter():
        yield b"foo"
        yield b"bar"

    cf.size = 0
    async for _ in _iter_and_add_size_to_cf(iter(), cf):
        pass

    assert cf.size == 6
