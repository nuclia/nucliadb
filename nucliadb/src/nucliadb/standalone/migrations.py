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

from nucliadb.common import locking
from nucliadb.common.cluster.standalone.utils import is_worker_node
from nucliadb.migrator.command import run as run_migrator


def run_migrations():
    """
    Run migrations for the standalone mode.
    """
    loop = asyncio.new_event_loop()
    loop.run_until_complete(safe_run_migrations())
    loop.close()


async def safe_run_migrations():
    """
    Run migrations for the standalone mode, only if the node is a worker node.
    The worker node will keep blocked until the migrations are run -- it relies
    on the migrator's internal distributed lock.
    """
    if not is_worker_node():
        return

    sys.stdout.write(
        """-------------------------------------------------
|   Running Migrations for NucliaDB Standalone
-------------------------------------------------
"""
    )
    while True:
        try:
            await run_migrator(forever=False)
            break
        except locking.ResourceLocked:
            sys.stdout.write("Another worker is already running migrations. Waiting...\n")
            continue
