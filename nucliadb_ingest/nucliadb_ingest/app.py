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
import logging
import signal
import sys
from asyncio import tasks
from typing import Callable, List

from nucliadb_ingest import logger, logger_activity
from nucliadb_ingest.consumer import start_consumer
from nucliadb_ingest.metrics import start_metrics
from nucliadb_ingest.partitions import assign_partitions
from nucliadb_ingest.sentry import SENTRY, set_sentry
from nucliadb_ingest.service import start_grpc
from nucliadb_ingest.settings import settings
from nucliadb_ingest.swim import start_swim
from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.settings import (
    indexing_settings,
    running_settings,
    transaction_settings,
)
from nucliadb_utils.transaction import LocalTransactionUtility, TransactionUtility
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_indexing,
    get_transaction,
    set_utility,
    start_audit_utility,
    stop_audit_utility,
)


async def start_transaction_utility():
    if transaction_settings.transaction_local:
        transaction_utility = LocalTransactionUtility()
    else:
        transaction_utility = TransactionUtility(
            nats_creds=transaction_settings.transaction_jetstream_auth,
            nats_servers=transaction_settings.transaction_jetstream_servers,
            nats_target=transaction_settings.transaction_jetstream_target,
        )
        await transaction_utility.initialize()
    set_utility(Utility.TRANSACTION, transaction_utility)


async def start_indexing_utility():
    indexing_utility = IndexingUtility(
        nats_creds=indexing_settings.index_jetstream_auth,
        nats_servers=indexing_settings.index_jetstream_servers,
        nats_target=indexing_settings.index_jetstream_target,
    )
    await indexing_utility.initialize()
    set_utility(Utility.INDEXING, indexing_utility)


async def stop_transaction_utility():
    transaction_utility = get_transaction()
    if transaction_utility:
        await transaction_utility.finalize()
        clean_utility(Utility.TRANSACTION)


async def stop_indexing_utility():
    indexing_utility = get_indexing()
    if indexing_utility:
        await indexing_utility.finalize()
        clean_utility(Utility.INDEXING)


async def main() -> List[Callable]:
    swim = start_swim()
    await start_transaction_utility()
    await start_indexing_utility()
    await start_audit_utility()
    grpc_finalizer = await start_grpc()
    consumer_finalizer = await start_consumer()
    metrics_finalizer = await start_metrics()
    logger.info(f"======= Ingest finished starting ======")
    finalizers = [
        grpc_finalizer,
        metrics_finalizer,
        consumer_finalizer,
        stop_transaction_utility,
        stop_indexing_utility,
        stop_audit_utility,
    ]

    if swim is not None:
        finalizers.append(swim.close)

    # Using ensure_future as Signal handlers
    # cannot handle couroutines as callbacks
    def stop_pulling():
        logger.info("Received signal to stop pulling!")
        asyncio.ensure_future(consumer_finalizer())

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGUSR1, stop_pulling)

    return finalizers


def _cancel_all_tasks(loop):
    to_cancel = tasks.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(tasks.gather(*to_cancel, return_exceptions=True))

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during asyncio.run() shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )


def run():
    if running_settings.sentry_url and SENTRY:
        set_sentry(
            running_settings.sentry_url,
            running_settings.running_environment,
            running_settings.logging_integration,
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)-18s | %(levelname)-7s | %(name)-16s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logger.setLevel(logging.getLevelName(running_settings.log_level.upper()))

    logger_activity.setLevel(
        logging.getLevelName(running_settings.activity_log_level.upper())
    )
    logging.getLogger("nucliadb_swim").setLevel(
        logging.getLevelName(running_settings.swim_level.upper())
    )

    logging.getLogger("asyncio").setLevel(logging.ERROR)

    if settings.logging_config:
        logging.config.fileConfig(
            settings.logging_config, disable_existing_loggers=True
        )

    assign_partitions(settings)

    if asyncio._get_running_loop() is not None:
        raise RuntimeError("cannot be called from a running event loop")

    loop = asyncio.new_event_loop()
    finalizers: List[Callable] = []
    try:
        asyncio.set_event_loop(loop)
        if running_settings.debug is not None:
            loop.set_debug(running_settings.debug)
        finalizers.extend(loop.run_until_complete(main()))

        if settings.monitor is True:
            import aiomonitor  # type: ignore

            with aiomonitor.start_monitor(
                loop=loop, host="0.0.0.0", port=settings.monitor_port
            ):
                loop.run_forever()
        else:
            loop.run_forever()
    finally:
        try:
            for finalizer in finalizers:
                if asyncio.iscoroutinefunction(finalizer):
                    loop.run_until_complete(finalizer())
                else:
                    finalizer()
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            loop.close()
