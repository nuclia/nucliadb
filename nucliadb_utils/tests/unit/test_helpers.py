# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
