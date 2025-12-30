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
import random
import uuid

import pytest

from nucliadb.common import locking


async def test_distributed_lock(maindb_driver):
    test_lock_key = uuid.uuid4().hex

    async def test_lock(for_seconds: float, lock_timeout: float = 1.0):
        async with locking.distributed_lock(
            test_lock_key,
            lock_timeout=lock_timeout,
            expire_timeout=0.5,
            refresh_timeout=0.2,
        ):
            await asyncio.sleep(for_seconds)

    task = asyncio.create_task(test_lock(1.5))
    await asyncio.sleep(0.05)
    assert await locking.is_locked(test_lock_key) is True
    with pytest.raises(locking.ResourceLocked):
        # should raise
        await test_lock(0.0)

    await task

    # get lock again now that it is free
    await test_lock(0.0)


async def test_distributed_lock_in_parallel(maindb_driver):
    """
    This tests that if multiple requests/tasks attempt to get the same lock,
    only one will eventually own it and the rest will get ResourceLocked error.
    """
    test_lock_key = uuid.uuid4().hex

    async def test_lock(for_seconds: float):
        async with locking.distributed_lock(
            test_lock_key,
            lock_timeout=0,
            expire_timeout=50,
            refresh_timeout=200,
        ):
            await asyncio.sleep(for_seconds)
            return for_seconds

    tasks = []
    for _ in range(5):
        tasks.append(asyncio.create_task(test_lock(random.uniform(0.1, 0.2))))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check that 4 out of 5 tasks returned ResourceLocked error
    locked_count = sum([1 if isinstance(r, locking.ResourceLocked) else 0 for r in results])
    assert locked_count == 4, f"{results}"
