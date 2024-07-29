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
import importlib.metadata
from typing import Awaitable, Callable

from nucliadb import health
from nucliadb.common.cluster.discovery.utils import (
    setup_cluster_discovery,
    teardown_cluster_discovery,
)
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.tasks import get_exports_consumer, get_imports_consumer
from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.consumer import service as consumer_service
from nucliadb.ingest.partitions import assign_partitions
from nucliadb.ingest.service import start_grpc
from nucliadb.ingest.settings import settings
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics
from nucliadb_utils.run import run_until_exit
from nucliadb_utils.settings import indexing_settings, transaction_settings
from nucliadb_utils.utilities import (
    start_audit_utility,
    start_indexing_utility,
    start_nats_manager,
    start_transaction_utility,
    stop_audit_utility,
    stop_indexing_utility,
    stop_nats_manager,
    stop_transaction_utility,
)


async def initialize() -> list[Callable[[], Awaitable[None]]]:
    await setup_telemetry(SERVICE_NAME)

    await setup_cluster()
    await start_transaction_utility(SERVICE_NAME)
    if not cluster_settings.standalone_mode and indexing_settings.index_jetstream_servers is not None:
        await start_indexing_utility(SERVICE_NAME)

    await start_audit_utility(SERVICE_NAME)

    finalizers = [
        stop_transaction_utility,
        stop_indexing_utility,
        stop_audit_utility,
        teardown_cluster,
    ]

    if not transaction_settings.transaction_local:
        # if we're running in standalone, we do not
        # want these services
        await start_nats_manager(
            SERVICE_NAME,
            transaction_settings.transaction_jetstream_servers,
            transaction_settings.transaction_jetstream_auth,
        )
        finalizers.append(stop_nats_manager)

        await setup_cluster_discovery()
        finalizers.append(teardown_cluster_discovery)

    health.register_health_checks(
        [
            health.nats_manager_healthy,
            health.nodes_health_check,
            health.pubsub_check,
        ]
    )

    return finalizers


async def initialize_grpc():  # pragma: no cover
    finalizers = await initialize()
    grpc_finalizer = await start_grpc(SERVICE_NAME)

    return [grpc_finalizer] + finalizers


async def initialize_pull_workers() -> list[Callable[[], Awaitable[None]]]:
    finalizers = await initialize_grpc()
    pull_workers = await consumer_service.start_pull_workers(SERVICE_NAME)

    return [pull_workers] + finalizers


async def main_consumer():  # pragma: no cover
    finalizers = await initialize()
    metrics_server = await serve_metrics()

    grpc_health_finalizer = await health.start_grpc_health_service(settings.grpc_port)

    # pull workers could be pulled out into it's own deployment
    pull_workers = await consumer_service.start_pull_workers(SERVICE_NAME)
    ingest_consumers = await consumer_service.start_ingest_consumers(SERVICE_NAME)

    await run_until_exit(
        [grpc_health_finalizer, pull_workers, ingest_consumers, metrics_server.shutdown] + finalizers
    )


async def main_orm_grpc():  # pragma: no cover
    finalizers = await initialize()
    grpc_finalizer = await start_grpc(SERVICE_NAME)
    metrics_server = await serve_metrics()
    await run_until_exit([grpc_finalizer, metrics_server.shutdown] + finalizers)


async def main_ingest_processed_consumer():  # pragma: no cover
    finalizers = await initialize()

    metrics_server = await serve_metrics()
    grpc_health_finalizer = await health.start_grpc_health_service(settings.grpc_port)
    consumer = await consumer_service.start_ingest_processed_consumer(SERVICE_NAME)

    await run_until_exit([grpc_health_finalizer, consumer, metrics_server.shutdown] + finalizers)


async def main_subscriber_workers():  # pragma: no cover
    finalizers = await initialize()

    context = ApplicationContext("subscriber-workers")
    await context.initialize()

    metrics_server = await serve_metrics()
    grpc_health_finalizer = await health.start_grpc_health_service(settings.grpc_port)
    auditor_closer = await consumer_service.start_auditor()
    shard_creator_closer = await consumer_service.start_shard_creator()
    materializer_closer = await consumer_service.start_materializer()

    exports_consumer = get_exports_consumer()
    await exports_consumer.initialize(context)
    imports_consumer = get_imports_consumer()
    await imports_consumer.initialize(context)

    await run_until_exit(
        [
            imports_consumer.finalize,
            exports_consumer.finalize,
            auditor_closer,
            shard_creator_closer,
            materializer_closer,
            metrics_server.shutdown,
            grpc_health_finalizer,
            context.finalize,
        ]
        + finalizers
    )


def setup_configuration():  # pragma: no cover
    setup_logging()
    assign_partitions(settings)

    errors.setup_error_handling(importlib.metadata.distribution("nucliadb").version)

    if asyncio._get_running_loop() is not None:
        raise RuntimeError("cannot be called from a running event loop")


def run_consumer() -> None:  # pragma: no cover
    """
    Running:
        - main consumer
        - pull worker
    """
    setup_configuration()
    asyncio.run(main_consumer())


def run_orm_grpc() -> None:  # pragma: no cover
    """
    Runs:
        - Ingest GRPC Service
    """
    setup_configuration()
    asyncio.run(main_orm_grpc())


def run_processed_consumer() -> None:  # pragma: no cover
    """
    Runs:
        - Consumer for processed messages from pull processor(CPU heavy)
    """
    setup_configuration()
    asyncio.run(main_ingest_processed_consumer())


def run_subscriber_workers() -> None:  # pragma: no cover
    """
    Runs:
        - shard creator subscriber
        - audit counter subscriber
        - audit fields subscriber
    """
    setup_configuration()
    asyncio.run(main_subscriber_workers())
