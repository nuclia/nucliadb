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

from nucliadb_ingest import logger
from nucliadb_ingest.consumer.pull import PullWorker
from nucliadb_ingest.maindb.driver import Driver
from nucliadb_ingest.settings import settings
from nucliadb_ingest.utils import get_driver
from nucliadb_utils.settings import nuclia_settings, transaction_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_audit, get_cache, get_storage


class ConsumerService:
    pull_workers_task: Dict[str, asyncio.Task]
    pull_workers: Dict[str, PullWorker]
    driver: Driver
    storage: Storage

    def __init__(
        self,
        partitions: Optional[List[int]] = None,
        pull_time: Optional[float] = None,
        zone: Optional[str] = None,
        creds: Optional[str] = None,
        nuclia_cluster_url: Optional[str] = None,
        nuclia_public_url: Optional[str] = None,
        nats_url: Optional[str] = None,
        nats_auth: Optional[str] = None,
        nats_target: Optional[str] = None,
        nats_group: Optional[str] = None,
        nats_stream: Optional[str] = None,
        onprem: Optional[bool] = None,
    ):
        self.partitions = partitions if partitions else settings.partitions
        self.pull_time = pull_time if pull_time else settings.pull_time

        self.zone = zone if zone else nuclia_settings.nuclia_zone
        self.nuclia_creds = creds if creds else nuclia_settings.nuclia_service_account
        self.nuclia_cluster_url = (
            nuclia_cluster_url
            if nuclia_cluster_url
            else nuclia_settings.nuclia_cluster_url
        )

        self.nuclia_public_url = (
            nuclia_public_url
            if nuclia_public_url
            else nuclia_settings.nuclia_public_url
        )

        self.nats_auth = (
            nats_auth if nats_auth else transaction_settings.transaction_jetstream_auth
        )
        self.nats_url = (
            nats_url if nats_url else transaction_settings.transaction_jetstream_servers
        )
        self.nats_target = (
            nats_target
            if nats_target
            else transaction_settings.transaction_jetstream_target
        )
        self.nats_group = (
            nats_group
            if nats_group
            else transaction_settings.transaction_jetstream_group
        )
        self.nats_stream = (
            nats_stream
            if nats_stream
            else transaction_settings.transaction_jetstream_stream
        )
        self.local_subscriber = transaction_settings.transaction_local
        self.onprem = onprem if onprem is not None else nuclia_settings.onprem
        self.pull_workers_task = {}
        self.pull_workers = {}
        self.audit = get_audit()

    async def run(self, service_name: Optional[str] = None):
        logger.info(
            f"Pulling from zone '{self.zone}' & partitions: {','.join(self.partitions)}"
        )

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
        self.storage = await get_storage()

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
            logger.exception("Consumer loop stoped by exception", exc_info=e)
