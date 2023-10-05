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
from nucliadb.tasks.logger import logger
from nucliadb.tasks.models import MsgType
from nucliadb.tasks.registry import get_registered_task
from nucliadb.tasks.utils import TaskCallback, create_nats_stream_if_not_exists
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.nats import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings

BEFORE_NAK_SLEEP_SECONDS = 2


class NatsTaskConsumer:
    def __init__(
        self,
        name: str,
        stream: const.Streams,
        callback: TaskCallback,
        msg_type: MsgType,
    ):
        self.name = name
        self.stream = stream
        self.callback = callback
        self.msg_type = msg_type
        self.initialized = False
        self.context: Optional[ApplicationContext] = None

    async def initialize(self, context: ApplicationContext):
        self.context = context
        await create_nats_stream_if_not_exists(
            self.context, self.stream.name, subjects=[self.stream.subject]  # type: ignore
        )
        await self._setup_nats_subscription()
        self.initialized = True

    async def _setup_nats_subscription(self):
        # Nats push consumer
        subject = self.stream.subject
        group = self.stream.group
        stream = self.stream.name
        await self.context.nats_manager.subscribe(
            subject=subject,
            queue=group,
            stream=stream,
            cb=self.subscription_worker,
            subscription_lost_cb=self._setup_nats_subscription,
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
                task_msg = self.msg_type.parse_raw(msg.data)
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

            logger.info(
                f"Starting task consumption", extra={"consumer_name": self.name}
            )
            try:
                await self.callback(self.context, task_msg)  # type: ignore
            except Exception as e:
                errors.capture_exception(e)
                logger.error(
                    f"Unexpected error while handling task",
                    extra={
                        "consumer_name": self.name,
                    },
                )
                # Nak the message to retry
                await asyncio.sleep(BEFORE_NAK_SLEEP_SECONDS)
                await msg.nak()
            else:
                logger.info(
                    f"Successful task",
                    extra={
                        "consumer_name": self.name,
                    },
                )
                await msg.ack()
            finally:
                return


def create_consumer(
    name: str,
    stream: const.Streams,
    callback: TaskCallback,
    msg_type: MsgType,
) -> NatsTaskConsumer:
    """
    Returns a non-initialized consumer
    """
    consumer = NatsTaskConsumer(
        name=name,
        stream=stream,
        callback=callback,
        msg_type=msg_type,
    )
    return consumer


async def start_consumer(
    task_name: str, context: ApplicationContext
) -> NatsTaskConsumer:
    """
    Returns an initialized consumer for the given task name, ready to consume messages from the task stream.
    """
    try:
        task = get_registered_task(task_name)
    except KeyError:
        raise ValueError(f"Task {task_name} not registered")
    consumer = create_consumer(
        name=f"{task_name}_consumer",
        stream=task.stream,
        callback=task.callback,  # type: ignore
        msg_type=task.msg_type,  # type: ignore
    )
    await consumer.initialize(context)
    return consumer
