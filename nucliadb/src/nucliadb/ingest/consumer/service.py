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
from collections.abc import Awaitable, Callable
from functools import partial

from nucliadb.common.maindb.utils import setup_driver
from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.consumer.consumer import IngestConsumer
from nucliadb.ingest.consumer.pull import PullV2Worker
from nucliadb.ingest.settings import settings
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.settings import transaction_settings
from nucliadb_utils.utilities import (
    get_audit,
    get_nats_manager,
    get_pubsub,
    get_storage,
)

from .auditing import IndexAuditHandler, ResourceWritesAuditHandler
from .materializer import MaterializerHandler
from .shard_creator import ShardCreatorHandler


def _handle_task_result(task: asyncio.Task) -> None:
    e = task.exception()
    if e:
        logger.exception("Loop stopped by exception. This should not happen. Exiting.", exc_info=e)
        sys.exit(1)


async def _exit_tasks(tasks: list[asyncio.Task]) -> None:
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def start_ingest_consumers(
    service_name: str | None = None,
) -> Callable[[], Awaitable[None]]:
    if transaction_settings.transaction_local:
        raise ConfigurationError("Can not start ingest consumers in local mode")

    driver = await setup_driver()
    pubsub = await get_pubsub()
    storage = await get_storage(service_name=service_name or SERVICE_NAME)
    nats_connection_manager = get_nats_manager()

    max_concurrent_processing = asyncio.Semaphore(settings.max_concurrent_ingest_processing)

    consumer_finalizers = []

    for partition in settings.partitions:
        consumer = IngestConsumer(
            driver=driver,
            partition=partition,
            storage=storage,
            pubsub=pubsub,
            nats_connection_manager=nats_connection_manager,
            lock=max_concurrent_processing,
        )
        await consumer.initialize()
        consumer_finalizers.append(consumer.finalize)

    async def _finalize():
        # Finalize all the consumers and the nats connection manager
        for consumer_finalize in consumer_finalizers:
            await consumer_finalize()
        await nats_connection_manager.finalize()

    return _finalize


async def start_ingest_processed_consumer_v2(
    service_name: str | None = None,
) -> Callable[[], Awaitable[None]]:
    """
    This is not meant to be deployed with a stateful set like the other consumers.

    We are not maintaining transactionability based on the nats sequence id from this
    consumer and we will start off by not separating writes by partition AND
    allowing NATS to manage the queue group for us.
    """
    driver = await setup_driver()
    pubsub = await get_pubsub()
    storage = await get_storage(service_name=service_name or SERVICE_NAME)

    consumer = PullV2Worker(
        driver=driver,
        storage=storage,
        pubsub=pubsub,
        pull_time_error_backoff=settings.pull_time_error_backoff,
        pull_api_timeout=settings.pull_api_timeout,
    )
    task = asyncio.create_task(consumer.loop())
    task.add_done_callback(_handle_task_result)
    return partial(_exit_tasks, [task])


async def start_auditor() -> Callable[[], Awaitable[None]]:
    audit = get_audit()
    assert audit is not None

    pubsub = await get_pubsub()
    assert pubsub is not None, "Pubsub is not configured"
    storage = await get_storage(service_name=SERVICE_NAME)
    index_auditor = IndexAuditHandler(audit=audit, pubsub=pubsub)
    resource_writes_auditor = ResourceWritesAuditHandler(storage=storage, audit=audit, pubsub=pubsub)

    await index_auditor.initialize()
    await resource_writes_auditor.initialize()

    return partial(
        asyncio.gather,
        index_auditor.finalize(),
        resource_writes_auditor.finalize(),  # type: ignore
    )


async def start_shard_creator() -> Callable[[], Awaitable[None]]:
    driver = await setup_driver()
    pubsub = await get_pubsub()
    assert pubsub is not None, "Pubsub is not configured"

    shard_creator = ShardCreatorHandler(driver=driver, pubsub=pubsub)
    await shard_creator.initialize()

    return shard_creator.finalize


async def start_materializer() -> Callable[[], Awaitable[None]]:
    driver = await setup_driver()
    pubsub = await get_pubsub()
    assert pubsub is not None, "Pubsub is not configured"
    storage = await get_storage(service_name=SERVICE_NAME)
    materializer = MaterializerHandler(driver=driver, storage=storage, pubsub=pubsub)
    await materializer.initialize()

    return materializer.finalize
