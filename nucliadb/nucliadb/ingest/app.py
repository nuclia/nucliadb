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
from typing import Awaitable, Callable, Optional, Union

import pkg_resources

from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.chitchat import start_chitchat, stop_chitchat
from nucliadb.ingest.consumer import start_consumer
from nucliadb.ingest.partitions import assign_partitions
from nucliadb.ingest.service import start_grpc
from nucliadb.ingest.settings import settings
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics
from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.run import run_until_exit
from nucliadb_utils.settings import indexing_settings, transaction_settings
from nucliadb_utils.transaction import LocalTransactionUtility, TransactionUtility
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_indexing,
    get_transaction_utility,
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
    transaction_utility = get_transaction_utility()
    if transaction_utility:
        await transaction_utility.finalize()
        clean_utility(Utility.TRANSACTION)


async def stop_indexing_utility():
    indexing_utility = get_indexing()
    if indexing_utility:
        await indexing_utility.finalize()
        clean_utility(Utility.INDEXING)


async def initialize() -> list[Callable[[], Awaitable[None]]]:
    await setup_telemetry(SERVICE_NAME)

    chitchat = await start_chitchat(SERVICE_NAME)

    await start_transaction_utility(SERVICE_NAME)
    await start_indexing_utility(SERVICE_NAME)
    await start_audit_utility(SERVICE_NAME)
    metrics_server = await serve_metrics()

    finalizers = [
        metrics_server.shutdown,
        stop_transaction_utility,
        stop_indexing_utility,
        stop_audit_utility,
        stop_chitchat,
    ]

    if chitchat is not None:
        finalizers.append(chitchat.close)

    return finalizers


async def initialize_consumer_and_grpc() -> list[Callable[[], Awaitable[None]]]:
    finalizers = await initialize()

    grpc_finalizer = await start_grpc(SERVICE_NAME)
    consumer_finalizer = await start_consumer(SERVICE_NAME)

    return [grpc_finalizer, consumer_finalizer] + finalizers


async def main_consumer():  # pragma: no cover
    finalizers = await initialize_consumer_and_grpc()
    await run_until_exit(finalizers)


async def main_orm_grpc():  # pragma: no cover
    finalizers = await initialize()
    grpc_finalizer = await start_grpc(SERVICE_NAME)
    await run_until_exit([grpc_finalizer] + finalizers)


def setup_configuration():  # pragma: no cover
    setup_logging()

    assign_partitions(settings)

    errors.setup_error_handling(pkg_resources.get_distribution("nucliadb").version)

    if asyncio._get_running_loop() is not None:
        raise RuntimeError("cannot be called from a running event loop")


def run_consumer() -> None:  # pragma: no cover
    """
    Run the consumer + GRPC ingest service
    """
    setup_configuration()
    asyncio.run(main_consumer())


def run_orm_grpc() -> None:  # pragma: no cover
    """
    Run the ingest GRPC service
    """
    setup_configuration()
    asyncio.run(main_orm_grpc())
