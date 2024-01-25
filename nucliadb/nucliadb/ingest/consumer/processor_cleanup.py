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

import logging
import uuid

from nucliadb.common.http_clients.processing import ProcessingHTTPClient
from nucliadb_protos import writer_pb2
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver

logger = logging.getLogger(__name__)


class ProcessorCleanupHandler:
    """
    The purpose of this component is to delete items in the processor
    that are no longer needed. This is done by subscribing to the pubsub
    notification channel and scheduling a task to delete the data.
    """

    subscription_id: str

    def __init__(self, *, pubsub: PubSubDriver):
        self.pubsub = pubsub
        self.client = ProcessingHTTPClient()

    async def initialize(self) -> None:
        self.subscription_id = str(uuid.uuid4())
        await self.pubsub.subscribe(
            handler=self.handle_message,
            key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid="*"),
            group="processor_cleanup",
            subscription_id=self.subscription_id,
        )

    async def finalize(self) -> None:
        await self.pubsub.unsubscribe(self.subscription_id)
        await self.client.close()

    async def handle_message(self, raw_data) -> None:
        data = self.pubsub.parse(raw_data)
        notification = writer_pb2.Notification()
        notification.ParseFromString(data)

        if notification.write_type != writer_pb2.Notification.WriteType.DELETED:
            # only handle deletes
            return

        await self.client.delete_requests(
            kbid=notification.kbid, resource_id=notification.uuid
        )
