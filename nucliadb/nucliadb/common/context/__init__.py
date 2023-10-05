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

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.cluster.settings import in_standalone_mode
from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_storage,
    start_indexing_utility,
    start_nats_manager,
    start_partitioning_utility,
    start_transaction_utility,
    stop_indexing_utility,
    stop_nats_manager,
    stop_partitioning_utility,
    stop_transaction_utility,
)


class ApplicationContext:
    kv_driver: Driver
    shard_manager: KBShardManager
    blob_storage: Storage
    partitioning: PartitionUtility
    indexing: IndexingUtility
    nats_manager: NatsConnectionManager

    def __init__(self, service_name: str = "service") -> None:
        self.service_name = service_name
        self._initialized: bool = False
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            await self._initialize()
            self._initialized = True

    async def _initialize(self):
        self.kv_driver = await setup_driver()
        self.blob_storage = await get_storage()
        self.shard_manager = await setup_cluster()
        self.partitioning = start_partitioning_utility()
        if not in_standalone_mode():
            self.nats_manager = await start_nats_manager(
                self.service_name,
                indexing_settings.index_jetstream_servers,
                indexing_settings.index_jetstream_auth,
            )
            self.indexing = await start_indexing_utility()
        self.transaction = await start_transaction_utility(self.service_name)

    async def finalize(self) -> None:
        if not self._initialized:
            return

        await stop_transaction_utility()
        if not in_standalone_mode():
            await stop_indexing_utility()
            await stop_nats_manager()
        stop_partitioning_utility()
        await teardown_cluster()
        await teardown_driver()
        await self.blob_storage.finalize()
        clean_utility(Utility.STORAGE)
        self._initialized = False
