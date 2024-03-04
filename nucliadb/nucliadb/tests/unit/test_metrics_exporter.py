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
from unittest import mock
from unittest.mock import AsyncMock, Mock

from nucliadb.metrics_exporter import run_exporter, run_exporter_task


async def test_run_exporter_task():
    coro = AsyncMock()
    task = asyncio.create_task(run_exporter_task(coro, interval=0.5))
    await asyncio.sleep(1)
    task.cancel()
    assert coro.await_count == 2


async def test_run_exporter():
    with mock.patch(
        "nucliadb.metrics_exporter.update_migration_metrics"
    ) as update_migration_metrics:
        with mock.patch(
            "nucliadb.metrics_exporter.update_node_metrics"
        ) as update_node_metrics:
            context = Mock()
            task = asyncio.create_task(run_exporter(context))

            await asyncio.sleep(1)

            update_node_metrics.assert_called()
            update_migration_metrics.assert_called()

            task.cancel()
