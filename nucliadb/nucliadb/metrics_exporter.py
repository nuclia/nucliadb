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

from nucliadb.common.cluster import manager
from nucliadb.common.context import ApplicationContext
from nucliadb_telemetry import metrics
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics

SHARD_COUNT = metrics.Gauge("nucliadb_node_shard_count", labels={"node": ""})


def update_node_metrics():
    all_nodes = manager.get_index_nodes()

    for node in all_nodes:
        if node.primary_id is not None:
            continue
        SHARD_COUNT.set(node.shard_count, labels=dict(node=node.id))


async def run_exporter():
    while True:
        await asyncio.sleep(10)
        update_node_metrics()


async def run(forever: bool = False):
    setup_logging()
    await setup_telemetry("metrics-exporter")

    context = ApplicationContext("metrics-exporter")
    await context.initialize()
    metrics_server = await serve_metrics()
    try:
        await run_exporter()
    finally:
        await context.finalize()
        await metrics_server.shutdown()


def main():
    asyncio.run(run())
