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
import sys
from functools import partial
from typing import Awaitable, Callable, Optional

from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.consumer.consumer import IngestConsumer, IngestProcessedConsumer
from nucliadb.ingest.consumer.pull import PullWorker
from nucliadb.ingest.orm import NODES
from nucliadb.ingest.settings import settings
from nucliadb.ingest.utils import get_driver
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.settings import (
    nuclia_settings,
    running_settings,
    transaction_settings,
)
from nucliadb_utils.utilities import get_audit, get_cache, get_nats_manager, get_storage


def _handle_task_result(task: asyncio.Task) -> None:
    e = task.exception()
    if e:
        logger.exception(
            "Loop stopped by exception. This should not happen. Exiting.", exc_info=e
        )
        sys.exit(1)


async def _exit_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def start_pull_workers(
    service_name: Optional[str] = None,
) -> Callable[[], Awaitable[None]]:
    driver = await get_driver()
    cache = await get_cache()
    storage = await get_storage(service_name=service_name or SERVICE_NAME)
    audit = get_audit()
    tasks = []
    for partition in settings.partitions:
        worker = PullWorker(
            driver=driver,
            partition=partition,
            storage=storage,
            pull_time_error_backoff=settings.pull_time_error_backoff,
            zone=nuclia_settings.nuclia_zone,
            cache=cache,
            audit=audit,
            creds=nuclia_settings.nuclia_service_account,
            nuclia_cluster_url=nuclia_settings.nuclia_cluster_url,
            nuclia_public_url=nuclia_settings.nuclia_public_url,
            onprem=nuclia_settings.onprem,
            local_subscriber=transaction_settings.transaction_local,
        )
        task = asyncio.create_task(worker.loop())
        task.add_done_callback(_handle_task_result)
        tasks.append(task)

    return partial(_exit_tasks, tasks)


async def start_ingest_consumers(
    service_name: Optional[str] = None,
) -> Callable[[], Awaitable[None]]:
    if transaction_settings.transaction_local:
        raise ConfigurationError("Can not start ingest consumers in local mode")

    while len(NODES) == 0 and running_settings.running_environment not in (
        "local",
        "test",
    ):
        logger.warning("Initializion delayed 1s to receive some Nodes on the cluster")
        await asyncio.sleep(1)

    driver = await get_driver()
    cache = await get_cache()
    storage = await get_storage(service_name=service_name or SERVICE_NAME)
    audit = get_audit()
    nats_connection_manager = get_nats_manager()

    for partition in settings.partitions:
        consumer = IngestConsumer(
            driver=driver,
            partition=partition,
            storage=storage,
            cache=cache,
            audit=audit,
            nats_connection_manager=nats_connection_manager,
        )
        await consumer.initialize()

    return nats_connection_manager.finalize


async def start_ingest_processed_consumer(
    service_name: Optional[str] = None,
) -> Callable[[], Awaitable[None]]:
    """
    This is not meant to be deployed with a stateful set like the other consumers.

    We are not maintaining transactionability based on the nats sequence id from this
    consumer and we will start off by not separating writes by partition AND
    allowing NATS to manage the queue group for us.
    """
    if transaction_settings.transaction_local:
        raise ConfigurationError("Can not start ingest consumers in local mode")

    while len(NODES) == 0 and running_settings.running_environment not in (
        "local",
        "test",
    ):
        logger.warning("Initializion delayed 1s to receive some Nodes on the cluster")
        await asyncio.sleep(1)

    driver = await get_driver()
    cache = await get_cache()
    storage = await get_storage(service_name=service_name or SERVICE_NAME)
    audit = get_audit()
    nats_connection_manager = get_nats_manager()

    consumer = IngestProcessedConsumer(
        driver=driver,
        partition="-1",
        storage=storage,
        cache=cache,
        audit=audit,
        nats_connection_manager=nats_connection_manager,
    )
    await consumer.initialize()

    return nats_connection_manager.finalize
