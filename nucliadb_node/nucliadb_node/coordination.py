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

import asyncio
from abc import ABC, abstractmethod
from enum import Enum
from functools import total_ordering


@total_ordering
class Priority(Enum):
    # Used for messages coming from processing, as they take long to process it
    # doesn't matter if we let some other messages be indexed before
    LOW = 0

    # High priority. Used for small messages coming from the writer that need to
    # be indexed before slow ones
    HIGH = 1

    def __eq__(self, other):
        return self.value.__eq__(other.value)

    def __lt__(self, other):
        return self.value.__lt__(other.value)

    def __hash__(self):
        return self.value.__hash__()


class QueueLock(ABC):
    """A queue lock represents a lockable resource with an specific release order"""

    @abstractmethod
    async def acquire(self):
        ...

    @abstractmethod
    async def release(self):
        ...

    @abstractmethod
    def locked(self):
        ...


class FifoLock(QueueLock):
    """Fifo lock queue relies on asyncio.Lock acquire *fair* behaviour to provide
    the desired order: first acquired will be the first released

    """

    def __init__(self):
        self.lock = asyncio.Lock()

    async def acquire(self, *args):
        await self.lock.acquire()

    async def release(self, *args):
        self.lock.release()

    def locked(self):
        return self.lock.locked()


class PriorityLock(QueueLock):
    def __init__(self):
        self.locks = {p: asyncio.Lock() for p in Priority}

    async def acquire(self, priority: Priority = Priority.LOW):
        await self.locks[priority].acquire()

    async def release(self, priority: Priority = Priority.LOW):
        for priority in reversed(sorted(Priority)):
            lock = self.locks[priority]
            if lock.locked():
                lock.release()
                break

    def locked(self, priority: Priority = Priority.LOW):
        for priority in Priority:
            if self.locks[priority].locked():
                return True
        return False


class ShardIndexingCoordinator:
    def __init__(self, lock_klass):
        self.lock = asyncio.Lock()
        self.shard_locks = {}
        self.lock_klass = lock_klass

    async def request_shard(self, shard_id: str):
        async with self.lock:
            lock, priority = self.shard_locks.setdefault(
                shard_id, (self.lock_klass(), Priority.LOW)
            )
        await lock.acquire(priority)

    async def request_shard_fast(self, shard_id: str):
        async with self.lock:
            lock, priority = self.shard_locks.setdefault(
                shard_id, (self.lock_klass(), Priority.HIGH)
            )
        await lock.acquire(priority)

    async def release_shard(self, shard_id: str):
        async with self.lock:
            lock, priority = self.shard_locks.get(shard_id)
            if lock is None:
                raise ValueError(f"Shard {shard_id} wasn't requested")

            if not lock.locked():
                self.shard_locks.pop(shard_id)

        await lock.release(priority)
