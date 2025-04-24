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
from typing import AsyncGenerator, Callable, Tuple, cast

from nucliadb import logger
from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.migrator.datamanager import MigrationsDataManager
from nucliadb_telemetry import metrics
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics

MIGRATION_COUNT = metrics.Gauge("nucliadb_migration", labels={"type": "", "version": ""})

PENDING_RESOURCE_COUNT = metrics.Gauge("nucliadb_pending_resources_count")


async def iter_kbids(context: ApplicationContext) -> AsyncGenerator[str, None]:
    """
    Return a list of all KB ids.
    """
    async with context.kv_driver.transaction(read_only=True) as txn:
        async for kbid, _ in datamanagers.kb.get_kbs(txn):
            yield kbid


async def update_migration_metrics(context: ApplicationContext):
    """
    Report the global migration version and the number of KBs per migration version.
    """
    # Clear previoulsy set values so that we report only the current state
    MIGRATION_COUNT.gauge.clear()

    mdm = MigrationsDataManager(context.kv_driver)
    global_info = await mdm.get_global_info()
    if global_info is not None:
        MIGRATION_COUNT.set(1, labels=dict(type="global", version=str(global_info.current_version)))

    version_count: dict[str, int] = {}
    async for kbid in iter_kbids(context):
        kb_info = await mdm.get_kb_info(kbid)
        if kb_info is not None:
            current_version = str(kb_info.current_version)
            version_count.setdefault(current_version, 0)
            version_count[current_version] += 1

    for version, count in version_count.items():
        MIGRATION_COUNT.set(count, labels=dict(type="kb", version=version))


async def update_resource_metrics(context: ApplicationContext):
    """
    Report the number of pending resources older than some estimated processing time
    """
    driver = get_driver()
    if not isinstance(driver, PGDriver):
        return

    async with driver._get_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT COUNT(*) FROM catalog "
            "WHERE labels @> '{/n/s/PENDING}' "
            "AND COALESCE(modified_at, created_at) BETWEEN NOW() - INTERVAL '1 month' AND NOW() - INTERVAL '6 hours'"
        )
        count = cast(Tuple[int], await cur.fetchone())[0]
        PENDING_RESOURCE_COUNT.set(count)


async def run_exporter_task(context: ApplicationContext, exporter_task: Callable, interval: int):
    """
    Run coroutine infinitely, catching exceptions and logging them.
    It will wait for the interval before running again.
    """
    try:
        while True:
            try:
                await exporter_task(context)
            except Exception:
                logger.error(f"Error on exporter task {exporter_task.__name__}", exc_info=True)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass


async def run_exporter(context: ApplicationContext):
    # Schedule exporter tasks
    tasks = []
    for export_task, interval in [
        (update_migration_metrics, 60 * 3),
        (update_resource_metrics, 60 * 5),
    ]:
        tasks.append(asyncio.create_task(run_exporter_task(context, export_task, interval=interval)))
    try:
        while True:
            await asyncio.sleep(10)
    except (asyncio.CancelledError, Exception):
        # Cancel all tasks
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
