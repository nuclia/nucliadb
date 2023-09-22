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

import nats
import pydantic
from nats.aio.client import Msg

from nucliadb.async_tasks import logger
from nucliadb.async_tasks.datamanager import AsyncTasksDataManager
from nucliadb.async_tasks.exceptions import (
    TaskMaxTriesReached,
    TaskNotFound,
    TaskShouldNotBeHandled,
)
from nucliadb.async_tasks.models import Status, Task, TaskCallbackState, TaskNatsMessage
from nucliadb.async_tasks.utils import TaskCallback
from nucliadb.common.context import ApplicationContext
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.nats import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings

UNRECOVERABLE_ERRORS = (TaskNotFound,)


class TaskLogger:
    def __init__(self, logger):
        self.logger = logger

    # TODO: put all logger task logic here
    pass


class NatsTaskConsumer:
    max_tries = 5

    def __init__(
        self,
        name: str,
        context: ApplicationContext,
        stream: const.Streams,
        callback: TaskCallback,
    ):
        self.name = name
        self.context = context
        self.stream = stream
        self.callback = callback
        self.initialized = False
        self.data_manager = AsyncTasksDataManager(context.kv_driver)

    async def initialize(self):
        await self.setup_nats_subscription()

    async def setup_nats_subscription(self):
        subject = self.stream.subject
        group = self.stream.group
        stream = self.stream.name
        await self.context.nats_manager.subscribe(
            subject=subject,
            queue=group,
            stream=stream,
            cb=self.subscription_worker,
            subscription_lost_cb=self.setup_nats_subscription,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.ALL,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                ack_wait=nats_consumer_settings.nats_ack_wait,
                idle_heartbeat=nats_consumer_settings.nats_idle_heartbeat,
            ),
        )
        logger.info(
            f"Subscribed to {subject} on stream {stream}",
            extra={"consumer_name": self.name},
        )

    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(reply.split(".")[5])
        logger.info(
            f"Message received: subject:{subject}, seqid: {seqid}, reply: {reply}",
            extra={"consumer_name": self.name},
        )
        async with MessageProgressUpdater(
            msg, nats_consumer_settings.nats_ack_wait * 0.66
        ):
            try:
                task_msg = TaskNatsMessage.parse_raw(msg.data)
                kbid, task_id = task_msg.kbid, task_msg.task_id
                task: Task = await self.data_manager.get_task(kbid, task_id)
            except pydantic.ValidationError as e:
                errors.capture_exception(e)
                logger.error(
                    "Invalid task message received", extra={"consumer_name": self.name}
                )
                await msg.ack()
                return
            except TaskNotFound:
                logger.error(
                    f"Task {task_id} not found for kbid {kbid}",
                    extra={"consumer_name": self.name},
                )
                await msg.ack()
                return
            try:
                await self.check_task_state(task)
            except TaskShouldNotBeHandled:
                logger.info(
                    f"Task {task.task_id} for {task.kbid} is {task.status.value}.",
                    extra={"consumer_name": self.name},
                )
                await msg.ack()
                return
            except TaskMaxTriesReached:
                logger.info(
                    f"Task {task.task_id} for kbid {task.kbid} failed too many times. Skipping",
                    extra={"consumer_name": self.name},
                )
                task.status = Status.ERRORED
                await self.data_manager.set_task(task)
                await msg.ack()
                return

            logger.info(
                f"Starting task {task_id} for kbid {kbid}",
                extra={"consumer_name": self.name},
            )
            task.status = Status.RUNNING
            task.tries += 1
            try:
                await self.callback(self.context, *task_msg.args, **task_msg.kwargs)
            except UNRECOVERABLE_ERRORS as e:
                # Unrecoverable errors are not retried
                errors.capture_exception(e)
                task.status = Status.ERRORED
                await msg.ack()
                return
            except Exception as e:
                errors.capture_exception(e)
                logger.error(
                    f"Error while handling task {task_id} for kbid {kbid}",
                    extra={"consumer_name": self.name},
                )
                await asyncio.sleep(2)
                task.status = Status.FAILED
                await msg.nak()
            else:
                # Successful task handling
                task.status = Status.FINISHED
                await msg.ack()
            finally:
                await self.data_manager.set_task(task)
                return

    async def check_task_state(self, task: Task):
        if task.status in (Status.FINISHED, Status.ERRORED, Status.CANCELLED):
            raise TaskShouldNotBeHandled()
        if task.tries >= self.max_tries:
            raise TaskMaxTriesReached()
        if task.status == Status.FAILED:
            logger.info(
                f"Retrying {task.task_id} for kbid {task.kbid} for the {task.tries + 1} time",
                extra={"consumer_name": self.name},
            )


async def export_kb(
    context: ApplicationContext,
    callback_state: TaskCallbackState,
    kbid: str,
    export_id: str,
):
    pass


async def main():
    context = ApplicationContext()
    await context.initialize()

    exporter_consumer = NatsTaskConsumer(
        name="export_consumer",
        context=context,
        stream=const.Streams.EXPORTS,
        callback=export_kb,
    )
    await exporter_consumer.initialize()


if __name__ == "__main__":
    asyncio.run(main())
