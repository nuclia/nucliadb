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

import pytest

from nucliadb_utils.cache import locking


@pytest.mark.asyncio
async def test_redis_distributed_lock(redis):
    url = f"redis://{redis[0]}:{redis[1]}"
    dist_lock_manager = locking.RedisDistributedLockManager(url, timeout=1)

    async def test_lock_works():
        async with dist_lock_manager.lock("test_lock"):
            await asyncio.sleep(1.1)

    async def test_locked():
        async with dist_lock_manager.lock("test_lock"):
            ...

    task = asyncio.create_task(test_lock_works())
    await asyncio.sleep(0.05)
    with pytest.raises(locking.ResourceLocked):
        await test_locked()

    await task

    # get lock again now that it is free
    await test_locked()
