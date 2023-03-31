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
from typing import Dict, List, Optional

from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.consumer.pull import PullWorker
from nucliadb.ingest.maindb.driver import Driver
from nucliadb.ingest.orm import NODES
from nucliadb.ingest.settings import settings
from nucliadb.ingest.utils import get_driver
from nucliadb_utils.settings import (
    nuclia_settings,
    running_settings,
    transaction_settings,
)
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_audit, get_cache, get_storage


class ConsumerService:
    pull_workers_task: Dict[str, asyncio.Task]
    pull_workers: Dict[str, PullWorker]
    driver: Driver
    storage: Storage

    def __init__(
        self,
        partitions: Optional[List[str]] = None,
        pull_time: Optional[int] = None,
        zone: Optional[str] = None,
        creds: Optional[str] = None,
        nuclia_cluster_url: Optional[str] = None,
        nuclia_public_url: Optional[str] = None,
        nats_servers: Optional[List[str]] = None,
        nats_auth: Optional[str] = None,
        nats_target: Optional[str] = None,
        nats_group: Optional[str] = None,
        nats_stream: Optional[str] = None,
        onprem: Optional[bool] = None,
    ):
        if partitions is not None:
            self.partitions: List[str] = partitions
        else:
            self.partitions = settings.partitions

        if pull_time is not None:
            self.pull_time: int = pull_time
        else:
            self.pull_time = settings.pull_time

        if zone is not None:
            self.zone: str = zone
        else:
            self.zone = nuclia_settings.nuclia_zone

        if creds is not None:
            self.nuclia_creds: Optional[str] = creds
        else:
            self.nuclia_creds = nuclia_settings.nuclia_service_account

        if nuclia_cluster_url is not None:
            self.nuclia_cluster_url: str = nuclia_cluster_url
        else:
            self.nuclia_cluster_url = nuclia_settings.nuclia_cluster_url

        self.nuclia_public_url = (
            nuclia_public_url
            if nuclia_public_url
            else nuclia_settings.nuclia_public_url
        )

        self.nats_auth = (
            nats_auth if nats_auth else transaction_settings.transaction_jetstream_auth
        )
        self.nats_url = (
            nats_servers
            if nats_servers is not None and len(nats_servers) > 0
            else transaction_settings.transaction_jetstream_servers
        )

        if nats_target is not None:
            self.nats_target: str = nats_target
        else:
            self.nats_target = transaction_settings.transaction_jetstream_target

        if nats_group is not None:
            self.nats_group: str = nats_group
        else:
            self.nats_group = transaction_settings.transaction_jetstream_group

        if nats_stream is not None:
            self.nats_stream: str = nats_stream
        else:
            self.nats_stream = transaction_settings.transaction_jetstream_stream

        self.local_subscriber = transaction_settings.transaction_local
        self.onprem = onprem if onprem is not None else nuclia_settings.onprem
        self.pull_workers_task = {}
        self.pull_workers = {}
        self.audit = get_audit()

    async def run(self, service_name: Optional[str] = None):
        logger.info(
            f"Ingest txn from zone '{self.zone}' & partitions: {','.join(self.partitions)}"
        )

        while len(NODES) == 0 and running_settings.running_environment not in (
            "local",
            "test",
        ):
            logger.warning(
                "Initializion delayed 1s to receive some Nodes on the cluster"
            )
            await asyncio.sleep(1)

        for partition in self.partitions:
            self.pull_workers[partition] = PullWorker(
                driver=self.driver,
                partition=partition,
                storage=self.storage,
                pull_time=self.pull_time,
                zone=self.zone,
                cache=self.cache,
                audit=self.audit,
                creds=self.nuclia_creds,
                nuclia_cluster_url=self.nuclia_cluster_url,
                nuclia_public_url=self.nuclia_public_url,
                target=self.nats_target,
                group=self.nats_group,
                stream=self.nats_stream,
                onprem=self.onprem,
                nats_creds=self.nats_auth,
                nats_servers=self.nats_url,
                local_subscriber=self.local_subscriber,
                service_name=service_name,
            )
            self.pull_workers_task[partition] = asyncio.create_task(
                self.pull_workers[partition].loop()
            )
            self.pull_workers_task[partition].add_done_callback(
                self._handle_task_result
            )

    async def start(self, service_name: Optional[str] = None):
        self.driver = await get_driver()
        self.cache = await get_cache()
        self.storage = await get_storage(service_name=SERVICE_NAME)

        # Start consummer coroutine
        await self.run(service_name)

    async def stop(self):
        for value in self.pull_workers.values():
            await value.finalize()
        for value in self.pull_workers_task.values():
            value.cancel()

    def _handle_task_result(self, task: asyncio.Task) -> None:
        e = task.exception()
        if e:
            logger.exception("Consumer loop stopped by exception", exc_info=e)
