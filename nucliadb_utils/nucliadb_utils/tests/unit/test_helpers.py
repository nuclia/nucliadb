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

from nucliadb_utils.helpers import async_gen_lookahead


async def test_async_gen_lookahead():
    async def gen(n):
        for i in range(n):
            yield f"{i}".encode()

    assert [item async for item in async_gen_lookahead(gen(0))] == []
    assert [item async for item in async_gen_lookahead(gen(1))] == [(b"0", True)]
    assert [item async for item in async_gen_lookahead(gen(2))] == [
        (b"0", False),
        (b"1", True),
    ]


async def test_async_gen_lookahead_last_chunk_is_empty():
    async def gen():
        for chunk in [b"empty", None, b"chunk", b""]:
            yield chunk

    assert [item async for item in async_gen_lookahead(gen())] == [
        (b"empty", False),
        (b"chunk", True),
    ]
