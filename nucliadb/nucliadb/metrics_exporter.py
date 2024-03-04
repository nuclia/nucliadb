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
from __future__ import annotations

import asyncio
import functools
from typing import Callable, Coroutine

from nucliadb import logger
from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.migrator.datamanager import MigrationsDataManager
from nucliadb_telemetry import metrics
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics

SHARD_COUNT = metrics.Gauge("nucliadb_node_shard_count", labels={"node": ""})

MIGRATION_COUNT = metrics.Gauge(
    "nucliadb_migration", labels={"type": "", "version": ""}
)


async def update_node_metrics():
    """
    Report the number of shards in each node.
    """
    all_nodes = cluster_manager.get_index_nodes()
    for node in all_nodes:
        if node.primary_id is not None:
            continue
        SHARD_COUNT.set(node.shard_count, labels=dict(node=node.id))


async def update_migration_metrics(context: ApplicationContext):
    """
    Report the global migration version and the number of KBs per migration version.
    """
    mdm = MigrationsDataManager(context.kv_driver)
    global_info = await mdm.get_global_info()
    if global_info is not None:
        MIGRATION_COUNT.set(
            1, labels=dict(type="global", version=str(global_info.current_version))
        )
    version_count: dict[str, int] = {}
    async with context.kv_driver.transaction() as txn:
        async for kbid, _ in KnowledgeBoxORM.get_kbs(txn, slug=""):
            kb_info = await mdm.get_kb_info(kbid)
            if kb_info is not None:
                version_count.setdefault(str(kb_info.current_version), 0)
                version_count[str(kb_info.current_version)] += 1
    for version, count in version_count.items():
        MIGRATION_COUNT.set(count, labels=dict(type="kb", version=version))


async def run_exporter_task(callable: Callable, interval: int):
    """
    Run coroutine infinitely, catching exceptions and logging them.
    It will wait for the interval before running again.
    """
    try:
        while True:
            try:
                await callable()
            except Exception:
                logger.error(
                    f"Error on exporter task {callable.__name__}", exc_info=True
                )
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass


async def run_exporter(context: ApplicationContext):
    tasks = []

    tasks.append(
        asyncio.create_task(run_exporter_task(update_node_metrics, interval=10))
    )

    update_migration_metrics_callable = functools.partial(
        update_migration_metrics, context
    )
    tasks.append(
        asyncio.create_task(
            run_exporter_task(update_migration_metrics_callable, interval=60 * 3)
        )
    )
    try:
        while True:
            await asyncio.sleep(10)
    except (asyncio.CancelledError, Exception):
        task: asyncio.Task
        for task in tasks:
            task.cancel()


async def run():
    setup_logging()
    await setup_telemetry("metrics-exporter")

    context = ApplicationContext("metrics-exporter")
    await context.initialize()
    metrics_server = await serve_metrics()
    try:
        await run_exporter(context)
    finally:
        await context.finalize()
        await metrics_server.shutdown()


def main():
    asyncio.run(run())
