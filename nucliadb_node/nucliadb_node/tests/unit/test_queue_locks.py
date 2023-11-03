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
import random
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pytest

from nucliadb_node.coordination import (
    FifoLock,
    Priority,
    PriorityLock,
    ShardIndexingCoordinator,
)

DEADLOCK_FIND_TESTS_DISABLE = False


class LockStatus(Enum):
    ACQUIRE = "acquired"
    RELEASE = "released"


class When(Enum):
    BEFORE = "before"
    AFTER = "after"


@dataclass
class Event:
    task: str
    when: When
    lock: LockStatus
    priority: Optional[Priority] = None


class EventRecorder:
    def __init__(self):
        self.events = []

    def record(self, event: Event):
        # useful for debugging if a deadlock is found. It's recommended to add
        # traces inside the tested queue lock too
        if DEADLOCK_FIND_TESTS_DISABLE is False:
            arrow = "--->" if event.when == When.BEFORE else "<---"
            print(f"[{event.task}] {arrow} {event.lock} with {event.priority}")
        self.events.append(event)


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

        events.record(Event("1", when=When.BEFORE, lock=LockStatus.ACQUIRE))
        await sic.request_shard(shard)
        events.record(Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("1", when=When.BEFORE, lock=LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("1", when=When.AFTER, lock=LockStatus.RELEASE))

    async def task_2():
        nonlocal events, shard, sic

        # let's task 1 start the sequence
        await asyncio.sleep(0)

        events.record(Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE))
        await sic.request_shard(shard)
        events.record(Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("2", when=When.BEFORE, lock=LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("2", when=When.AFTER, lock=LockStatus.RELEASE))

    async def task_3():
        nonlocal events, shard, sic

        # let's tasks 1 and 2 start the sequence
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        events.record(Event("3", when=When.BEFORE, lock=LockStatus.ACQUIRE))
        await sic.request_shard(shard)
        events.record(Event("3", when=When.AFTER, lock=LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("3", when=When.BEFORE, lock=LockStatus.RELEASE))
        await sic.release_shard(shard)
        events.record(Event("3", when=When.AFTER, lock=LockStatus.RELEASE))

    await asyncio.gather(task_1(), task_2(), task_3())

    expected = [
        Event("1", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("3", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("1", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("1", when=When.AFTER, lock=LockStatus.RELEASE),
        Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("2", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("2", when=When.AFTER, lock=LockStatus.RELEASE),
        Event("3", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("3", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("3", when=When.AFTER, lock=LockStatus.RELEASE),
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

        events.record(Event("1", when=When.BEFORE, lock=LockStatus.ACQUIRE))
        await sic.request_shard("shard-1")
        events.record(Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("1", when=When.BEFORE, lock=LockStatus.RELEASE))
        await sic.release_shard("shard-1")
        events.record(Event("1", when=When.AFTER, lock=LockStatus.RELEASE))

    async def task_2():
        nonlocal events, sic

        # let's task 1 start the sequence
        await asyncio.sleep(0)

        events.record(Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE))
        await sic.request_shard("shard-2")
        events.record(Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE))

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(Event("2", when=When.BEFORE, lock=LockStatus.RELEASE))
        await sic.release_shard("shard-2")
        events.record(Event("2", when=When.AFTER, lock=LockStatus.RELEASE))

    await asyncio.gather(task_1(), task_2())

    expected = [
        Event("1", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("1", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("1", when=When.AFTER, lock=LockStatus.RELEASE),
        Event("2", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("2", when=When.AFTER, lock=LockStatus.RELEASE),
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

        events.record(
            Event("1", when=When.BEFORE, lock=LockStatus.ACQUIRE, priority=Priority.LOW)
        )
        await sic.request_shard(shard, Priority.LOW)
        events.record(
            Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE, priority=Priority.LOW)
        )

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(
            Event("1", when=When.BEFORE, lock=LockStatus.RELEASE, priority=Priority.LOW)
        )
        await sic.release_shard(shard)
        events.record(
            Event("1", when=When.AFTER, lock=LockStatus.RELEASE, priority=Priority.LOW)
        )

    async def task_2():
        nonlocal events, shard, sic

        # let's task 1 start the sequence
        await asyncio.sleep(0)

        events.record(
            Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE, priority=Priority.LOW)
        )
        await sic.request_shard(shard, Priority.LOW)
        events.record(
            Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE, priority=Priority.LOW)
        )

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(
            Event("2", when=When.BEFORE, lock=LockStatus.RELEASE, priority=Priority.LOW)
        )
        await sic.release_shard(shard)
        events.record(
            Event("2", when=When.AFTER, lock=LockStatus.RELEASE, priority=Priority.LOW)
        )

    async def task_3():
        nonlocal events, shard, sic

        # let's tasks 1 and 2 start the sequence
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        events.record(
            Event(
                "3", when=When.BEFORE, lock=LockStatus.ACQUIRE, priority=Priority.HIGH
            )
        )
        await sic.request_shard(shard, Priority.HIGH)
        events.record(
            Event("3", when=When.AFTER, lock=LockStatus.ACQUIRE, priority=Priority.HIGH)
        )

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(
            Event(
                "3", when=When.BEFORE, lock=LockStatus.RELEASE, priority=Priority.HIGH
            )
        )
        await sic.release_shard(shard)
        events.record(
            Event("3", when=When.AFTER, lock=LockStatus.RELEASE, priority=Priority.HIGH)
        )

    await asyncio.gather(task_1(), task_2(), task_3())

    expected = [
        Event("1", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("3", when=When.BEFORE, lock=LockStatus.ACQUIRE),
        Event("1", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("1", when=When.AFTER, lock=LockStatus.RELEASE),
        # 3 has requested with priority, so it will go before 2
        Event("3", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("3", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("3", when=When.AFTER, lock=LockStatus.RELEASE),
        Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE),
        Event("2", when=When.BEFORE, lock=LockStatus.RELEASE),
        Event("2", when=When.AFTER, lock=LockStatus.RELEASE),
    ]
    assert events.events == expected


@pytest.mark.parametrize("lock_klass", [PriorityLock])
@pytest.mark.asyncio
async def test_shard_indexing_coordinator_single_shard_with_priority_high_first(
    lock_klass,
):
    """Test how coordination works with a priority queue lock and 2 tasks competing
    for the same shard.

    We expect tasks to synchronize and make progress one after the other in the
    request shard order.

    """
    sic = ShardIndexingCoordinator(lock_klass)
    shard = "my-shard"
    events = EventRecorder()

    async def task_1():
        nonlocal events, shard, sic

        events.record(
            Event(
                "1", when=When.BEFORE, lock=LockStatus.ACQUIRE, priority=Priority.HIGH
            )
        )
        await sic.request_shard(shard, Priority.HIGH)
        events.record(
            Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE, priority=Priority.HIGH)
        )

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(
            Event(
                "1", when=When.BEFORE, lock=LockStatus.RELEASE, priority=Priority.HIGH
            )
        )
        await sic.release_shard(shard)
        events.record(
            Event("1", when=When.AFTER, lock=LockStatus.RELEASE, priority=Priority.HIGH)
        )

    async def task_2():
        nonlocal events, shard, sic

        # let's task 1 start the sequence
        await asyncio.sleep(0)

        events.record(
            Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE, priority=Priority.LOW)
        )
        await sic.request_shard(shard, Priority.LOW)
        events.record(
            Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE, priority=Priority.LOW)
        )

        # simulate some async work
        await asyncio.sleep(0.010)

        events.record(
            Event("2", when=When.BEFORE, lock=LockStatus.RELEASE, priority=Priority.LOW)
        )
        await sic.release_shard(shard)
        events.record(
            Event("2", when=When.AFTER, lock=LockStatus.RELEASE, priority=Priority.LOW)
        )

    print()
    await asyncio.gather(task_1(), task_2())

    expected = [
        Event("1", when=When.BEFORE, lock=LockStatus.ACQUIRE, priority=Priority.HIGH),
        Event("1", when=When.AFTER, lock=LockStatus.ACQUIRE, priority=Priority.HIGH),
        Event("2", when=When.BEFORE, lock=LockStatus.ACQUIRE, priority=Priority.LOW),
        Event("1", when=When.BEFORE, lock=LockStatus.RELEASE, priority=Priority.HIGH),
        Event("1", when=When.AFTER, lock=LockStatus.RELEASE, priority=Priority.HIGH),
        Event("2", when=When.AFTER, lock=LockStatus.ACQUIRE, priority=Priority.LOW),
        Event("2", when=When.BEFORE, lock=LockStatus.RELEASE, priority=Priority.LOW),
        Event("2", when=When.AFTER, lock=LockStatus.RELEASE, priority=Priority.LOW),
    ]
    assert events.events == expected


@pytest.mark.skipif(
    DEADLOCK_FIND_TESTS_DISABLE, reason="Run when modifying risky logic"
)
@pytest.mark.parametrize("lock_klass", [PriorityLock])
@pytest.mark.asyncio
async def test_shard_indexing_coordinator_single_shard_with_priority_deadlock_finder(
    lock_klass,
):
    sic = ShardIndexingCoordinator(lock_klass)
    shard = "my-shard"
    events = EventRecorder()

    async def task(task_id: str, cycles: int):
        nonlocal events, shard, sic

        priorities = list(iter(Priority))

        for _ in range(cycles):
            idx = random.randint(0, len(priorities) - 1)
            priority = priorities[idx]

            events.record(
                Event(
                    task_id,
                    when=When.BEFORE,
                    lock=LockStatus.ACQUIRE,
                    priority=priority,
                )
            )
            await sic.request_shard(shard, priority)
            events.record(
                Event(
                    task_id, when=When.AFTER, lock=LockStatus.ACQUIRE, priority=priority
                )
            )

            # simulate some async work
            await asyncio.sleep(random.randint(0, 5) * 0.0001)

            events.record(
                Event(
                    task_id,
                    when=When.BEFORE,
                    lock=LockStatus.RELEASE,
                    priority=priority,
                )
            )
            await sic.release_shard(shard)
            events.record(
                Event(
                    task_id, when=When.AFTER, lock=LockStatus.RELEASE, priority=priority
                )
            )

    for _ in range(100):
        for task_count in range(50):
            cycles = 100
            await asyncio.gather(
                *[task(str(i), cycles) for i in range(1, task_count + 1)]
            )

            assert len(events.events) == cycles * task_count * 4
            events.events.clear()
