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
from unittest.mock import MagicMock

import pytest

from nucliadb_utils.retry import backoff_retry_async_generator
import backoff


async def test_backoff_retry_async_generator_ok():
    mock = MagicMock()

#    @backoff_retry_async_generator((ValueError,), max_tries=2)
    @backoff.on_exception(backoff.expo, ValueError, max_tries=2)
    async def gen(arg, kwarg=None):
        mock(arg, kwarg=kwarg)
        yield 1

    assert [i async for i in gen("foo", kwarg="bar")] == [1]
    assert mock.call_count == 1
    mock.assert_called_once_with("foo", kwarg="bar")


async def test_backoff_retry_async_generator_retries_on_error():
    mock = MagicMock()

#    @backoff_retry_async_generator((ValueError,), max_tries=2)
    @backoff.on_exception(backoff.expo, ValueError, max_tries=2)
    async def gen():
        mock()
        raise ValueError()
        yield 1

    with pytest.raises(ValueError):
        async for _ in gen():
            pass

    assert mock.call_count == 2


# async def test_backoff_retry_async_generator_retries_single_error():
#     mock = MagicMock()
#     iterator = list(range(4))
#     @backoff_retry_async_generator((ValueError,), max_tries=2)
#     async def gen(iterator):
#         while len(iterator) > 0:
#             i = iterator.pop(0)
#             if i == 1 and mock.call_count == 0:
#                 mock(i)
#                 breakpoint()
#                 iterator = [i] + iterator
#                 raise ValueError()
#             yield i

#     assert [i async for i in gen(iterator)] == [0, 1, 2, 3]
#     assert mock.call_count == 1
