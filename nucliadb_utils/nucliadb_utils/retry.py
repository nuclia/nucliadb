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
import functools
from typing import AsyncGenerator, Iterable, Type

from nucliadb_utils import logger


def backoff_retry_async_generator(
    retriable_exceptions: Iterable[Type[Exception]],
    *,
    max_tries: int,
):
    def decorator(func):
        @functools.wraps(func)
        async def inner(*args, **kwargs):
            interval = 2
            retry = 0
            while True:
                try:
                    async for item in func(*args, **kwargs):
                        yield item
                    break
                except Exception as e:
                    if e.__class__ not in retriable_exceptions:
                        raise e

                    if retry >= max_tries - 1:
                        raise e

                    backoff_time = interval**retry
                    logger.warning(
                        f"Retrying ({retry} of {max_tries}) with interval {interval} in {backoff_time} seconds"
                    )
                    await asyncio.sleep(backoff_time)
                    retry += 1

        return inner

    return decorator
