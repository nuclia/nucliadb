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
from typing import Generic, Type

from nucliadb.tasks.logger import logger
from nucliadb.tasks.models import MsgType
from nucliadb.tasks.utils import NatsStream
from nucliadb_telemetry import errors
from nucliadb_utils.utilities import get_nats_manager


class NatsTaskProducer(Generic[MsgType]):
    def __init__(
        self,
        name: str,
        stream: NatsStream,
        producer_subject: str,
        msg_type: Type[MsgType],
    ):
        self.name = name
        self.stream = stream
        self.producer_subject = producer_subject
        self.msg_type = msg_type
        self.nats_manager = get_nats_manager()

    async def send(self, msg: MsgType) -> int:
        """
        Publish message to the producer's nats stream.
        Returns the sequence number of the published message.
        """
        try:
            pub_ack = await self.nats_manager.js.publish(
                self.producer_subject,
                msg.model_dump_json().encode("utf-8"),
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
    stream: NatsStream,
    producer_subject: str,
    msg_type: Type[MsgType],
) -> NatsTaskProducer[MsgType]:
    """
    Returns a non-initialized producer.
    """
    producer = NatsTaskProducer[MsgType](
        name=name,
        stream=stream,
        producer_subject=producer_subject,
        msg_type=msg_type,
    )
    return producer
