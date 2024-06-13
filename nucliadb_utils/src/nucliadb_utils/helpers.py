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


async def async_gen_lookahead(
    gen: AsyncGenerator[bytes, None],
) -> AsyncGenerator[tuple[bytes, bool], None]:
    """Async generator that yields the next chunk and whether it's the last one.
    Empty chunks are ignored.

    """
    buffered_chunk = None
    async for chunk in gen:
        if buffered_chunk is None:
            # Buffer the first chunk
            buffered_chunk = chunk
            continue

        if chunk is None or len(chunk) == 0:
            continue

        # Yield the previous chunk and buffer the current one
        yield buffered_chunk, False
        buffered_chunk = chunk

    # Yield the last chunk if there is one
    if buffered_chunk is not None:
        yield buffered_chunk, True
