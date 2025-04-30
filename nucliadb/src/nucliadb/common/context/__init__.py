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

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.cluster.settings import in_standalone_mode
from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.common.nidx import NidxUtility, start_nidx_utility, stop_nidx_utility
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.transaction import TransactionUtility
from nucliadb_utils.utilities import (
    get_storage,
    start_nats_manager,
    start_partitioning_utility,
    start_transaction_utility,
    stop_nats_manager,
    stop_partitioning_utility,
    stop_transaction_utility,
    teardown_storage,
)


class ApplicationContext:
    def __init__(
        self,
        service_name: str = "service",
        kv_driver: bool = True,
        blob_storage: bool = True,
        shard_manager: bool = True,
        partitioning: bool = True,
        nats_manager: bool = True,
        transaction: bool = True,
        nidx: bool = True,
    ) -> None:
        self.service_name = service_name
        self._initialized: bool = False
        self._lock = asyncio.Lock()
        self._kv_driver: Optional[Driver] = None
        self._blob_storage: Optional[Storage] = None
        self._shard_manager: Optional[KBShardManager] = None
        self._partitioning: Optional[PartitionUtility] = None
        self._nats_manager: Optional[NatsConnectionManager] = None
        self._transaction: Optional[TransactionUtility] = None
        self._nidx: Optional[NidxUtility] = None
        self.enabled_kv_driver = kv_driver
        self.enabled_blob_storage = blob_storage
        self.enabled_shard_manager = shard_manager
        self.enabled_partitioning = partitioning
        self.enabled_nats_manager = nats_manager
        self.enabled_transaction = transaction
        self.enabled_nidx = nidx

    async def initialize(self) -> None:
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            await self._initialize()
            self._initialized = True

    async def _initialize(self):
        if self.enabled_kv_driver:
            self._kv_driver = await setup_driver()
        if self.enabled_blob_storage:
            self._blob_storage = await get_storage()
        if self.enabled_shard_manager:
            self._shard_manager = await setup_cluster()
        if self.enabled_partitioning:
            self._partitioning = start_partitioning_utility()
        if not in_standalone_mode() and self.enabled_nats_manager:
            self._nats_manager = await start_nats_manager(
                self.service_name,
                indexing_settings.index_jetstream_servers,
                indexing_settings.index_jetstream_auth,
            )
        if self.enabled_transaction:
            self._transaction = await start_transaction_utility(self.service_name)
        if self.enabled_nidx:
            self._nidx = await start_nidx_utility(self.service_name)

    @property
    def kv_driver(self) -> Driver:
        assert self._kv_driver is not None, "Driver not initialized"
        return self._kv_driver

    @property
    def shard_manager(self) -> KBShardManager:
        assert self._shard_manager is not None, "Shard manager not initialized"
        return self._shard_manager

    @property
    def blob_storage(self) -> Storage:
        assert self._blob_storage is not None, "Blob storage not initialized"
        return self._blob_storage

    @property
    def partitioning(self) -> PartitionUtility:
        assert self._partitioning is not None, "Partitioning not initialized"
        return self._partitioning

    @property
    def nats_manager(self) -> NatsConnectionManager:
        assert self._nats_manager is not None, "NATS manager not initialized"
        return self._nats_manager

    @property
    def transaction(self) -> TransactionUtility:
        assert self._transaction is not None, "Transaction utility not initialized"
        return self._transaction

    @property
    def nidx(self) -> NidxUtility:
        assert self._nidx is not None, "Nidx utility not initialized"
        return self._nidx

    async def finalize(self) -> None:
        if not self._initialized:
            return
        if self.enabled_nidx:
            await stop_nidx_utility()
        if self.enabled_transaction:
            await stop_transaction_utility()
        if not in_standalone_mode() and self.enabled_nats_manager:
            await stop_nats_manager()
        if self.enabled_partitioning:
            stop_partitioning_utility()
        if self.enabled_shard_manager:
            await teardown_cluster()
        if self.enabled_blob_storage:
            await teardown_storage()
        if self.enabled_kv_driver:
            await teardown_driver()
        self._initialized = False
