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
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.cluster.settings import settings as cluster_settings
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
    stop_indexing_utility,
    stop_nats_manager,
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

    async def initialize(self) -> None:
        self.kv_driver = await setup_driver()
        self.blob_storage = await get_storage()
        self.shard_manager = await setup_cluster()
        self.partitioning = start_partitioning_utility()
        if not cluster_settings.standalone_mode:
            self.indexing = await start_indexing_utility()
        self.nats_manager = await start_nats_manager(
            self.service_name,
            indexing_settings.index_jetstream_servers,
            indexing_settings.index_jetstream_auth,
        )

    async def finalize(self) -> None:
        await teardown_driver()
        await teardown_cluster()
        await self.blob_storage.finalize()
        await stop_indexing_utility()
        await stop_nats_manager()
        clean_utility(Utility.STORAGE)
        clean_utility(Utility.PARTITION)
