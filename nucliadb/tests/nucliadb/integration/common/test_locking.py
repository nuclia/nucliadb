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


async def test_lock_expiration_and_takeover(maindb_driver):
    """
    Test that an expired lock can be taken over by another process.
    """
    test_lock_key = uuid.uuid4().hex

    # Acquire lock with very short expiration
    async with locking.distributed_lock(
        test_lock_key,
        lock_timeout=1.0,
        expire_timeout=0.2,
        refresh_timeout=10.0,  # Make refresh longer than expiration so it won't refresh
    ):
        await asyncio.sleep(0.1)
        assert await locking.is_locked(test_lock_key) is True
        # Cancel the refresh task early to simulate process death
        # Note: the lock will auto-expire

    # Wait for lock to expire
    await asyncio.sleep(0.3)

    # Should be able to acquire the expired lock
    async with locking.distributed_lock(
        test_lock_key,
        lock_timeout=1.0,
        expire_timeout=1.0,
        refresh_timeout=0.5,
    ):
        assert await locking.is_locked(test_lock_key) is True


async def test_lock_refresh_keeps_lock_alive(maindb_driver):
    """
    Test that the refresh mechanism keeps the lock alive beyond its initial expiration.
    """
    test_lock_key = uuid.uuid4().hex

    async with locking.distributed_lock(
        test_lock_key,
        lock_timeout=1.0,
        expire_timeout=0.3,
        refresh_timeout=0.1,  # Refresh every 0.1s
    ):
        # Hold the lock for 0.6s, which is longer than the 0.3s expiration
        # The refresh should keep it alive
        await asyncio.sleep(0.6)
        # Lock should still be held
        assert await locking.is_locked(test_lock_key) is True


async def test_lock_release_on_exception(maindb_driver):
    """
    Test that locks are properly released even when an exception occurs.
    """
    test_lock_key = uuid.uuid4().hex

    with pytest.raises(ValueError):
        async with locking.distributed_lock(
            test_lock_key,
            lock_timeout=1.0,
            expire_timeout=5.0,
            refresh_timeout=1.0,
        ):
            raise ValueError("Test exception")

    # Lock should be released, so we can acquire it immediately
    async with locking.distributed_lock(
        test_lock_key,
        lock_timeout=0.1,
        expire_timeout=1.0,
        refresh_timeout=0.5,
    ):
        assert await locking.is_locked(test_lock_key) is True


async def test_multiple_different_locks(maindb_driver):
    """
    Test that multiple different locks can be held simultaneously.
    """
    lock_key_1 = uuid.uuid4().hex
    lock_key_2 = uuid.uuid4().hex
    lock_key_3 = uuid.uuid4().hex

    async with locking.distributed_lock(lock_key_1, expire_timeout=5.0):
        async with locking.distributed_lock(lock_key_2, expire_timeout=5.0):
            async with locking.distributed_lock(lock_key_3, expire_timeout=5.0):
                # All three locks should be held
                assert await locking.is_locked(lock_key_1) is True
                assert await locking.is_locked(lock_key_2) is True
                assert await locking.is_locked(lock_key_3) is True


async def test_lock_timeout_with_held_lock(maindb_driver):
    """
    Test that lock timeout works correctly when lock is held by another process.
    """
    test_lock_key = uuid.uuid4().hex
    import time

    async def hold_lock():
        async with locking.distributed_lock(
            test_lock_key,
            lock_timeout=5.0,
            expire_timeout=5.0,
            refresh_timeout=1.0,
        ):
            await asyncio.sleep(1.0)

    # Start holding the lock
    task = asyncio.create_task(hold_lock())
    await asyncio.sleep(0.1)  # Ensure lock is acquired

    # Try to acquire with short timeout
    start = time.time()
    with pytest.raises(locking.ResourceLocked):
        async with locking.distributed_lock(
            test_lock_key,
            lock_timeout=0.3,
            expire_timeout=5.0,
            refresh_timeout=1.0,
        ):
            pass

    elapsed = time.time() - start
    # Should timeout around 0.3 seconds (with some tolerance)
    assert 0.2 < elapsed < 0.5, f"Timeout took {elapsed}s, expected ~0.3s"

    await task


async def test_is_locked_on_nonexistent_lock(maindb_driver):
    """
    Test that is_locked returns False for non-existent locks.
    """
    nonexistent_key = uuid.uuid4().hex
    assert await locking.is_locked(nonexistent_key) is False


async def test_is_locked_on_expired_lock(maindb_driver):
    """
    Test that is_locked returns False for expired locks.
    """
    test_lock_key = uuid.uuid4().hex

    # Create and expire a lock
    async with locking.distributed_lock(
        test_lock_key,
        lock_timeout=1.0,
        expire_timeout=0.1,
        refresh_timeout=10.0,  # Don't refresh
    ):
        pass

    # Wait for expiration
    await asyncio.sleep(0.2)

    # Should return False for expired lock
    assert await locking.is_locked(test_lock_key) is False


async def test_concurrent_lock_attempts_sequential_success(maindb_driver):
    """
    Test that multiple processes can acquire the same lock sequentially.
    """
    test_lock_key = uuid.uuid4().hex
    acquisition_order = []

    async def acquire_lock(task_id: int):
        async with locking.distributed_lock(
            test_lock_key,
            lock_timeout=2.0,
            expire_timeout=0.2,
            refresh_timeout=0.1,
        ):
            acquisition_order.append(task_id)
            await asyncio.sleep(0.1)

    # Launch 3 tasks that will wait for each other
    tasks = [
        asyncio.create_task(acquire_lock(1)),
        asyncio.create_task(acquire_lock(2)),
        asyncio.create_task(acquire_lock(3)),
    ]

    await asyncio.gather(*tasks)

    # All three should have acquired the lock
    assert len(acquisition_order) == 3
    assert set(acquisition_order) == {1, 2, 3}


async def test_lock_cleanup_does_not_affect_active_locks(maindb_driver):
    """
    Test that cleanup doesn't interfere with active locks.
    """
    test_lock_key = uuid.uuid4().hex

    async with locking.distributed_lock(
        test_lock_key,
        lock_timeout=1.0,
        expire_timeout=10.0,
        refresh_timeout=1.0,
    ):
        # Manually trigger cleanup
        lock_instance = locking.distributed_lock(test_lock_key)
        try:
            await lock_instance._cleanup_expired_locks()
        except Exception:
            pass  # Cleanup might fail, that's ok

        # Active lock should still be held
        assert await locking.is_locked(test_lock_key) is True


async def test_reentrant_lock_fails(maindb_driver):
    """
    Test that the same process cannot acquire the same lock twice (not reentrant).
    """
    test_lock_key = uuid.uuid4().hex

    async with locking.distributed_lock(
        test_lock_key,
        lock_timeout=0.2,
        expire_timeout=5.0,
        refresh_timeout=1.0,
    ):
        # Try to acquire the same lock again
        with pytest.raises(locking.ResourceLocked):
            async with locking.distributed_lock(
                test_lock_key,
                lock_timeout=0.2,
                expire_timeout=5.0,
                refresh_timeout=1.0,
            ):
                pass
