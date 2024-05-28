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
from typing import Optional

from nucliadb.common.context import ApplicationContext
from nucliadb.tasks.logger import logger
from nucliadb.tasks.models import MsgType
from nucliadb.tasks.registry import get_registered_task
from nucliadb.tasks.utils import create_nats_stream_if_not_exists
from nucliadb_telemetry import errors
from nucliadb_utils import const


class NatsTaskProducer:
    def __init__(
        self,
        name: str,
        stream: const.Streams,
        msg_type: MsgType,
    ):
        self.name = name
        self.stream = stream
        self.msg_type = msg_type
        self.context: Optional[ApplicationContext] = None
        self.initialized = False

    async def initialize(self, context: ApplicationContext):
        self.context = context
        await create_nats_stream_if_not_exists(
            self.context, self.stream.name, subjects=[self.stream.subject]  # type: ignore
        )
        self.initialized = True

    async def __call__(self, msg: MsgType) -> int:  # type: ignore
        """
        Publish message to the producer's nats stream.
        Returns the sequence number of the published message.
        """
        if not self.initialized:
            raise RuntimeError("NatsTaskProducer not initialized")
        try:
            pub_ack = await self.context.nats_manager.js.publish(  # type: ignore
                self.stream.subject, msg.json().encode("utf-8")  # type: ignore
            )
            logger.info(
                "Message sent to Nats",
                extra={"producer_name": self.name},
            )
        except Exception as e:
            errors.capture_exception(e)
            logger.error(
                "Error sending message to Nats",
                extra={"producer_name": self.name},
            )
            raise
        else:
            return pub_ack.seq


def create_producer(
    name: str,
    stream: const.Streams,
    msg_type: MsgType,
) -> NatsTaskProducer:
    """
    Returns a non-initialized producer.
    """
    return NatsTaskProducer(name=name, stream=stream, msg_type=msg_type)


async def get_producer(task_name: str, context: ApplicationContext) -> NatsTaskProducer:
    """
    Returns a producer for the given task name, ready to be used to send messages to the task stream.
    """
    try:
        task = get_registered_task(task_name)
    except KeyError:
        raise ValueError(f"Task {task_name} not registered")
    producer = create_producer(
        name=f"{task_name}_producer", stream=task.stream, msg_type=task.msg_type
    )
    await producer.initialize(context)
    return producer
