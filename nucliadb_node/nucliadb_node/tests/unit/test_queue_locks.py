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
from dataclasses import dataclass
from enum import Enum

import pytest

from nucliadb_node.coordination import (
    FifoLock,
    Priority,
    PriorityLock,
    ShardIndexingCoordinator,
)


class LockStatus(Enum):
    ACQUIRE = "acquired"
    RELEASE = "released"


class When(Enum):
    BEFORE = "before"
    AFTER = "after"


@dataclass
class Event:
    task: str
    lock: LockStatus
    when: When


class EventRecorder:
    def __init__(self):
        self._events = []

    @property
    def events(self):
        return self._events

    def record(self, event: Event):
        self._events.append(event)


def test_priority_ordering():
    assert Priority.LOW < Priority.HIGH

    expected = [Priority.LOW, Priority.HIGH]
    for a, b in zip(expected, sorted(Priority)):
        assert a == b


@pytest.mark.parametrize("lock_klass", [FifoLock, PriorityLock])
@pytest.mark.asyncio
async def test_shard_indexing_coordinator_single_shard(lock_klass):
    """Test how coordination works with a queue lock and 3 tasks competing for the
    same shard.

    We expect tasks to synchronize and make progress one after the other in the
    request shard order.

    """
    sic = ShardIndexingCoordinator(lock_klass)
    shard = "my-shard"
    events = EventRecorder()

    async def task_1():
        nonlocal events, shard, sic

        events.record(Event("1", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard(shard)
        events.record(Event("1", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("1", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("1", When.AFTER, LockStatus.RELEASE))

    async def task_2():
        nonlocal events, shard, sic

        # let's task 1 start the sequence
        await asyncio.sleep(0)

        events.record(Event("2", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard(shard)
        events.record(Event("2", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("2", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("2", When.AFTER, LockStatus.RELEASE))

    async def task_3():
        nonlocal events, shard, sic

        # let's tasks 1 and 2 start the sequence
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        events.record(Event("3", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard(shard)
        events.record(Event("3", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("3", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("3", When.AFTER, LockStatus.RELEASE))

    await asyncio.gather(task_1(), task_2(), task_3())

    expected = [
        Event("1", When.BEFORE, LockStatus.ACQUIRE),
        Event("1", When.AFTER, LockStatus.ACQUIRE),
        Event("2", When.BEFORE, LockStatus.ACQUIRE),
        Event("3", When.BEFORE, LockStatus.ACQUIRE),
        Event("1", When.BEFORE, LockStatus.RELEASE),
        Event("1", When.AFTER, LockStatus.RELEASE),
        Event("2", When.AFTER, LockStatus.ACQUIRE),
        Event("2", When.BEFORE, LockStatus.RELEASE),
        Event("2", When.AFTER, LockStatus.RELEASE),
        Event("3", When.AFTER, LockStatus.ACQUIRE),
        Event("3", When.BEFORE, LockStatus.RELEASE),
        Event("3", When.AFTER, LockStatus.RELEASE),
    ]
    assert events.events == expected


@pytest.mark.parametrize("lock_klass", [FifoLock, PriorityLock])
@pytest.mark.asyncio
async def test_shard_indexing_coordinator_multiple_shards(
    lock_klass,
):
    """Test how coordination works with a queue lock and 2 tasks competing for
    different shards.

    The expected behaviour is a non blocking scenario where both tasks can get
    and release the locks independently.

    """
    sic = ShardIndexingCoordinator(lock_klass)
    events = EventRecorder()

    async def task_1():
        nonlocal events, sic

        events.record(Event("1", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard("shard-1")
        events.record(Event("1", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("1", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard("shard-1")
        events.record(Event("1", When.AFTER, LockStatus.RELEASE))

    async def task_2():
        nonlocal events, sic

        # let's task 1 start the sequence
        await asyncio.sleep(0)

        events.record(Event("2", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard("shard-2")
        events.record(Event("2", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("2", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard("shard-2")
        events.record(Event("2", When.AFTER, LockStatus.RELEASE))

    await asyncio.gather(task_1(), task_2())

    expected = [
        Event("1", When.BEFORE, LockStatus.ACQUIRE),
        Event("1", When.AFTER, LockStatus.ACQUIRE),
        Event("2", When.BEFORE, LockStatus.ACQUIRE),
        Event("2", When.AFTER, LockStatus.ACQUIRE),
        Event("1", When.BEFORE, LockStatus.RELEASE),
        Event("1", When.AFTER, LockStatus.RELEASE),
        Event("2", When.BEFORE, LockStatus.RELEASE),
        Event("2", When.AFTER, LockStatus.RELEASE),
    ]
    assert events.events == expected


@pytest.mark.parametrize("lock_klass", [PriorityLock])
@pytest.mark.asyncio
async def test_shard_indexing_coordinator_single_shard_with_priority(lock_klass):
    """Test how coordination works with a priority queue lock and 3 tasks competing
    for the same shard.

    We expect tasks to synchronize and make progress one after the other in the
    request shard order.

    """
    sic = ShardIndexingCoordinator(lock_klass)
    shard = "my-shard"
    events = EventRecorder()

    async def task_1():
        nonlocal events, shard, sic

        events.record(Event("1", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard(shard, Priority.LOW)
        events.record(Event("1", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("1", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("1", When.AFTER, LockStatus.RELEASE))

    async def task_2():
        nonlocal events, shard, sic

        # let's task 1 start the sequence
        await asyncio.sleep(0)

        events.record(Event("2", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard(shard, Priority.LOW)
        events.record(Event("2", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("2", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("2", When.AFTER, LockStatus.RELEASE))

    async def task_3():
        nonlocal events, shard, sic

        # let's tasks 1 and 2 start the sequence
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        events.record(Event("3", When.BEFORE, LockStatus.ACQUIRE))
        await sic.request_shard(shard, Priority.HIGH)
        events.record(Event("3", When.AFTER, LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("3", When.BEFORE, LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("3", When.AFTER, LockStatus.RELEASE))

    await asyncio.gather(task_1(), task_2(), task_3())

    expected = [
        Event("1", When.BEFORE, LockStatus.ACQUIRE),
        Event("1", When.AFTER, LockStatus.ACQUIRE),
        Event("2", When.BEFORE, LockStatus.ACQUIRE),
        Event("3", When.BEFORE, LockStatus.ACQUIRE),
        Event("1", When.BEFORE, LockStatus.RELEASE),
        Event("1", When.AFTER, LockStatus.RELEASE),
        # 3 has requested with priority, so it will go before 2
        Event("3", When.AFTER, LockStatus.ACQUIRE),
        Event("3", When.BEFORE, LockStatus.RELEASE),
        Event("3", When.AFTER, LockStatus.RELEASE),
        Event("2", When.AFTER, LockStatus.ACQUIRE),
        Event("2", When.BEFORE, LockStatus.RELEASE),
        Event("2", When.AFTER, LockStatus.RELEASE),
    ]
    assert events.events == expected
