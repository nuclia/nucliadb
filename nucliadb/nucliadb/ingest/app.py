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
from typing import Callable, List, Optional, Union

from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat

from nucliadb.ingest import SERVICE_NAME, logger, logger_activity
from nucliadb.ingest.chitchat import start_chitchat
from nucliadb.ingest.consumer import start_consumer
from nucliadb.ingest.metrics import start_metrics
from nucliadb.ingest.partitions import assign_partitions
from nucliadb.ingest.service import start_grpc
from nucliadb.ingest.settings import settings
from nucliadb.sentry import SENTRY, set_sentry
from nucliadb_telemetry.utils import get_telemetry, init_telemetry
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


async def start_transaction_utility(service_name: Optional[str] = None):
    if transaction_settings.transaction_local:
        transaction_utility: Union[
            LocalTransactionUtility, TransactionUtility
        ] = LocalTransactionUtility()
    elif (
        transaction_settings.transaction_jetstream_servers is not None
        and transaction_settings.transaction_jetstream_target is not None
    ):
        transaction_utility = TransactionUtility(
            nats_creds=transaction_settings.transaction_jetstream_auth,
            nats_servers=transaction_settings.transaction_jetstream_servers,
            nats_target=transaction_settings.transaction_jetstream_target,
            notify_subject=transaction_settings.transaction_notification,
        )
        await transaction_utility.initialize(service_name)
    set_utility(Utility.TRANSACTION, transaction_utility)


async def start_indexing_utility(service_name: Optional[str] = None):
    if (
        not indexing_settings.index_local
        and indexing_settings.index_jetstream_servers is not None
        and indexing_settings.index_jetstream_target is not None
    ):
        indexing_utility = IndexingUtility(
            nats_creds=indexing_settings.index_jetstream_auth,
            nats_servers=indexing_settings.index_jetstream_servers,
            nats_target=indexing_settings.index_jetstream_target,
        )
        await indexing_utility.initialize(service_name)
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
    set_logging()
    tracer_provider = get_telemetry(SERVICE_NAME)
    if tracer_provider is not None:
        set_global_textmap(B3MultiFormat())
        await init_telemetry(tracer_provider)  # To start asyncio task

    chitchat = await start_chitchat(SERVICE_NAME)

    await start_transaction_utility(SERVICE_NAME)
    await start_indexing_utility(SERVICE_NAME)
    await start_audit_utility()
    grpc_finalizer = await start_grpc(SERVICE_NAME)
    consumer_finalizer = await start_consumer(SERVICE_NAME)
    metrics_finalizer = await start_metrics()

    finalizers = [
        grpc_finalizer,
        metrics_finalizer,
        consumer_finalizer,
        stop_transaction_utility,
        stop_indexing_utility,
        stop_audit_utility,
    ]

    if chitchat is not None:
        finalizers.append(chitchat.close)

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


def set_logging():
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
    logging.getLogger("nucliadb_chitchat").setLevel(
        logging.getLevelName(running_settings.chitchat_level.upper())
    )

    logging.getLogger("asyncio").setLevel(logging.ERROR)

    if settings.logging_config:
        logging.config.fileConfig(
            settings.logging_config, disable_existing_loggers=True
        )

    assign_partitions(settings)


def run() -> None:
    if running_settings.sentry_url and SENTRY:
        set_sentry(
            running_settings.sentry_url,
            running_settings.running_environment,
            running_settings.logging_integration,
        )

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
