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
from typing import Generic, Optional, Type

from nucliadb.common.context import ApplicationContext
from nucliadb.tasks.logger import logger
from nucliadb.tasks.models import MsgType
from nucliadb.tasks.utils import create_nats_stream_if_not_exists
from nucliadb_telemetry import errors


class NatsTaskProducer(Generic[MsgType]):
    def __init__(
        self,
        name: str,
        stream: str,
        stream_subjects: list[str],
        producer_subject: str,
        msg_type: Type[MsgType],
    ):
        self.name = name
        self.stream = stream
        self.stream_subjects = stream_subjects
        self.producer_subject = producer_subject
        self.msg_type = msg_type
        self.context: Optional[ApplicationContext] = None
        self.initialized = False

    async def initialize(self, context: ApplicationContext):
        self.context = context
        await create_nats_stream_if_not_exists(
            self.context,
            self.stream,
            subjects=self.stream_subjects,
        )
        self.initialized = True

    async def send(self, msg: MsgType) -> int:
        """
        Publish message to the producer's nats stream.
        Returns the sequence number of the published message.
        """
        if not self.initialized:
            raise RuntimeError("NatsTaskProducer not initialized")
        try:
            pub_ack = await self.context.nats_manager.js.publish(  # type: ignore
                self.producer_subject,
                msg.model_dump_json().encode("utf-8"),  # type: ignore
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
    stream: str,
    stream_subjects: list[str],
    producer_subject: str,
    msg_type: Type[MsgType],
) -> NatsTaskProducer[MsgType]:
    """
    Returns a non-initialized producer.
    """
    producer = NatsTaskProducer[MsgType](
        name=name,
        stream=stream,
        stream_subjects=stream_subjects,
        producer_subject=producer_subject,
        msg_type=msg_type,
    )
    return producer
