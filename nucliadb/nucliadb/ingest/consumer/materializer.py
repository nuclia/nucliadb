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
from functools import partial

from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.maindb.driver import Driver
from nucliadb_protos import writer_pb2
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.storages.storage import Storage

from .utils import DelayedTaskHandler

logger = logging.getLogger(__name__)


class MaterializerHandler:
    """
    The purpose of this component is to materialize data that is
    expensive to calculate at read time and is okay to be stale.

    This component will only materialize data on kbs that are
    being written to. This is done by subscribing to the pubsub
    notification channel and scheduling a task to materialize
    the data. The task will be scheduled with a delay to allow
    for multiple resources to be written but a single materialization
    will be done.
    """

    subscription_id: str

    def __init__(
        self,
        *,
        driver: Driver,
        pubsub: PubSubDriver,
        storage: Storage,
        check_delay: float = 30.0,
    ):
        self.resources_data_manager = ResourcesDataManager(driver, storage)
        self.pubsub = pubsub
        self.shard_manager = get_shard_manager()
        self.task_handler = DelayedTaskHandler(check_delay)

    async def initialize(self) -> None:
        self.subscription_id = str(uuid.uuid4())
        await self.task_handler.initialize()
        await self.pubsub.subscribe(
            handler=self.handle_message,
            key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid="*"),
            group="materializer",
            subscription_id=self.subscription_id,
        )

    async def finalize(self) -> None:
        await self.pubsub.unsubscribe(self.subscription_id)
        await self.task_handler.finalize()

    async def handle_message(self, raw_data) -> None:
        data = self.pubsub.parse(raw_data)
        notification = writer_pb2.Notification()
        notification.ParseFromString(data)

        if (
            notification.action
            != writer_pb2.Notification.Action.COMMIT  # only on commits
            or notification.write_type
            == writer_pb2.Notification.WriteType.MODIFIED  # only on new resources and deletes
        ):
            return

        self.task_handler.schedule(
            notification.kbid, partial(self.process, notification.kbid)
        )

    async def process(self, kbid: str) -> None:
        logger.info(f"Materializing knowledgebox", extra={"kbid": kbid})
        await self.resources_data_manager.set_number_of_resources(
            kbid, await self.resources_data_manager.calculate_number_of_resources(kbid)
        )
