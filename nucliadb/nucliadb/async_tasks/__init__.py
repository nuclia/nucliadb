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

from nucliadb.async_tasks.exceptions import TaskCancelled, TaskErrored, TaskNotFound
from nucliadb.common.context import ApplicationContext

from .consumer import NatsTaskConsumer, create_consumer
from .datamanager import AsyncTasksDataManager
from .producer import NatsTaskProducer, create_producer
from .registry import register_task  # noqa
from .registry import get_registered_task


async def start_consumer(
    task_name: str, context: ApplicationContext
) -> NatsTaskConsumer:
    """
    Returns an initialized consumer for the given task name, ready to consume messages from the task stream.
    """
    try:
        registry_data = get_registered_task(task_name)
    except KeyError:
        raise ValueError(f"Task {task_name} not registered")
    consumer = create_consumer(
        name=f"{task_name}_consumer",
        stream=registry_data["stream"],
        callback=registry_data["func"],
        max_retries=registry_data["max_retries"],
    )
    await consumer.initialize(context)
    return consumer


async def get_producer(task_name: str, context: ApplicationContext) -> NatsTaskProducer:
    """
    Returns a producer for the given task name, ready to be used to send messages to the task stream.
    """
    try:
        registry_data = get_registered_task(task_name)
    except KeyError:
        raise ValueError(f"Task {task_name} not registered")
    producer = create_producer(
        name=f"{task_name}_producer",
        stream=registry_data["stream"],
    )
    await producer.initialize(context)
    return producer


async def wait_for_task(
    kbid: str, task_id: str, context: ApplicationContext, max_wait: int = 100
):
    """
    Waits for the given task to finish, or raises an exception if it fails.
    """
    finished = False
    dm = AsyncTasksDataManager(context.kv_driver)
    for _ in range(max_wait):
        try:
            task = await dm.get_task(kbid, task_id)
        except TaskNotFound:
            await asyncio.sleep(1)
            continue
        if task.status == "cancelled":
            raise TaskCancelled()
        if task.status == "errored":
            raise TaskErrored()
        if task.status != "finished":
            await asyncio.sleep(1)
            continue
        finished = True
        break
    assert finished
