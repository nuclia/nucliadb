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
from typing import Optional

import nats
import pydantic
from nats.aio.client import Msg

from nucliadb.common.context import ApplicationContext
from nucliadb.tasks.datamanager import AsyncTasksDataManager
from nucliadb.tasks.exceptions import (
    TaskMaxTriesReached,
    TaskNotFoundError,
    TaskShouldNotBeHandled,
)
from nucliadb.tasks.logger import logger
from nucliadb.tasks.models import Task, TaskNatsMessage, TaskStatus
from nucliadb.tasks.utils import TaskCallback
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.nats import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings

UNRECOVERABLE_ERRORS = (TaskNotFoundError,)


class NatsTaskConsumer:
    def __init__(
        self,
        name: str,
        stream: const.Streams,
        callback: TaskCallback,
        max_retries: Optional[int] = None,
    ):
        self.name = name
        self.stream = stream
        self.callback = callback
        self.max_retries = max_retries
        self.initialized = False
        self.context: Optional[ApplicationContext] = None
        self._dm = None

    @property
    def dm(self):
        if self._dm is None:
            self._dm = AsyncTasksDataManager(self.context.kv_driver)
        return self._dm

    async def initialize(self, context: ApplicationContext):
        self.context = context
        await self.setup_nats_subscription()
        self.initialized = True

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
                task: Task = await self.dm.get_task(kbid, task_id)
            except pydantic.ValidationError as e:
                errors.capture_exception(e)
                logger.error(
                    "Invalid task message received",
                    extra={
                        "consumer_name": self.name,
                    },
                )
                await msg.ack()
                return
            except TaskNotFoundError:
                logger.error(
                    f"Task not found",
                    extra={
                        "consumer_name": self.name,
                        "kbid": kbid,
                        "task_id": task_id,
                    },
                )
                await msg.ack()
                return

            try:
                await self.check_task_state(task)
            except TaskShouldNotBeHandled:
                logger.info(
                    f"Task will not be handled. TaskStatus: {task.status.value}",
                    extra={
                        "consumer_name": self.name,
                        "kbid": kbid,
                        "task_id": task_id,
                    },
                )
                await msg.ack()
                return
            except TaskMaxTriesReached:
                logger.info(
                    f"Task failed too many times. Skipping",
                    extra={
                        "consumer_name": self.name,
                        "kbid": kbid,
                        "task_id": task_id,
                    },
                )
                task.status = TaskStatus.ERRORED
                await self.dm.set_task(task)
                await msg.ack()
                return

            logger.info(
                f"Starting task",
                extra={"consumer_name": self.name, "kbid": kbid, "task_id": task_id},
            )
            task.status = TaskStatus.RUNNING
            task.tries += 1
            try:
                await self.dm.set_task(task)
                await self.callback(
                    self.context, kbid, *task_msg.args, **task_msg.kwargs  # type: ignore
                )
            except UNRECOVERABLE_ERRORS as e:
                errors.capture_exception(e)
                # Unrecoverable errors are not retried
                task.status = TaskStatus.ERRORED
                await msg.ack()
                return
            except Exception as e:
                errors.capture_exception(e)
                logger.error(
                    f"Unexpected error while handling task",
                    extra={
                        "consumer_name": self.name,
                        "kbid": kbid,
                        "task_id": task_id,
                    },
                )
                await asyncio.sleep(2)
                # Set status to failed so that the task is retried
                task.status = TaskStatus.FAILED
                await msg.nak()
            else:
                logger.info(
                    f"Successful task",
                    extra={
                        "consumer_name": self.name,
                        "kbid": kbid,
                        "task_id": task_id,
                    },
                )
                task.status = TaskStatus.FINISHED
                await msg.ack()
            finally:
                await self.dm.set_task(task)
                return

    async def check_task_state(self, task: Task):
        if task.status in (
            TaskStatus.FINISHED,
            TaskStatus.ERRORED,
            TaskStatus.CANCELLED,
        ):
            raise TaskShouldNotBeHandled()
        if self.max_retries is not None and task.tries > self.max_retries:
            raise TaskMaxTriesReached()
        if task.status == TaskStatus.FAILED:
            logger.info(
                f"Retrying {task.task_id} for kbid {task.kbid} for the {task.tries + 1} time",
                extra={"consumer_name": self.name},
            )


def create_consumer(
    name: str,
    stream: const.Streams,
    callback: TaskCallback,
    max_retries: Optional[int] = None,
) -> NatsTaskConsumer:
    consumer = NatsTaskConsumer(
        name=name,
        stream=stream,
        callback=callback,
        max_retries=max_retries,
    )
    return consumer
