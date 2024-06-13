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

from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics

from . import migrator
from .context import ExecutionContext
from .exceptions import MigrationValidationError
from .settings import Settings
from .utils import get_migrations


async def run(forever: bool = False):
    setup_logging()
    await setup_telemetry("migrator")

    settings = Settings()
    context = ExecutionContext(settings)
    await context.initialize()
    metrics_server = await serve_metrics()
    try:
        if forever:
            await migrator.run_forever(context)
        else:
            await migrator.run(context)
    finally:
        await context.finalize()
        await metrics_server.shutdown()


def validate():
    migrations = get_migrations()
    versions = set()
    for migration in migrations:
        if migration.version in versions:
            raise MigrationValidationError(f"Migration {migration.version} is duplicated")
        versions.add(migration.version)


def main():
    asyncio.run(run())


def main_forever():
    """
    Keep running migrations forever and retry all failures.
    """
    asyncio.run(run(forever=True))
