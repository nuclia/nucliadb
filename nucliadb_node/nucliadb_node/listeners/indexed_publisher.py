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

from functools import partial

from nucliadb_protos.nodewriter_pb2 import IndexMessage
from nucliadb_protos.writer_pb2 import Notification

from nucliadb_node import logger, signals
from nucliadb_node.signals import SuccessfulIndexingPayload
from nucliadb_utils import const
from nucliadb_utils.utilities import get_pubsub


class IndexedPublisher:
    listener_id = "indexed-publisher-{id}"

    def __init__(self):
        self.pubsub = None

    async def initialize(self):
        self.pubsub = await get_pubsub()
        signals.successful_indexing.add_listener(
            self.listener_id.format(id=id(self)),
            partial(IndexedPublisher.on_successful_indexing, self),
        )

    async def finalize(self):
        await self.pubsub.finalize()
        signals.successful_indexing.remove_listener(
            self.listener_id.format(id=id(self))
        )

    async def on_successful_indexing(self, payload: SuccessfulIndexingPayload):
        await self.indexed(payload.index_message)

    async def indexed(self, indexpb: IndexMessage):
        if not indexpb.HasField("partition"):
            logger.warning("Could not publish message without partition")
            return

        message = Notification(
            partition=int(indexpb.partition),
            seqid=indexpb.txid,
            uuid=indexpb.resource,
            kbid=indexpb.kbid,
            action=Notification.INDEXED,
        )

        await self.pubsub.publish(
            const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=indexpb.kbid),
            message.SerializeToString(),
        )
