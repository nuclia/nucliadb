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
from typing import Optional

from nucliadb.migrator.context import ExecutionContext
from nucliadb.migrator.utils import get_migrations
from nucliadb_telemetry import errors, metrics

migration_observer = metrics.Observer(
    "nucliadb_migrations", labels={"type": "kb", "target_version": ""}
)


logger = logging.getLogger(__name__)


async def run_kb_migrations(
    context: ExecutionContext, kbid: str, target_version: int
) -> None:
    async with context.maybe_distributed_lock(f"migration-{kbid}"):
        kb_info = await context.data_manager.get_kb_info(kbid)
        if kb_info is None:
            logger.warning("KB not found", extra={"kbid": kbid})
            await context.data_manager.delete_kb_migration(kbid=kbid)
            return

        migrations = get_migrations(
            from_version=kb_info.current_version, to_version=target_version
        )

        for migration in migrations:
            migration_info = {
                "from_version": kb_info.current_version,
                "to_version": migration.version,
                "kbid": kbid,
            }

            try:
                logger.warning("Migrating KB", extra=migration_info)
                with migration_observer(
                    {"type": "kb", "target_version": str(migration.version)}
                ):
                    await migration.module.migrate_kb(context, kbid)  # type: ignore
                logger.warning("Finished KB Migration", extra=migration_info)
                await context.data_manager.update_kb_info(
                    kbid=kbid, current_version=migration.version
                )
            except Exception as exc:
                errors.capture_exception(exc)
                logger.exception("Failed to migrate KB", extra=migration_info)
                raise

        refreshed_kb_info = await context.data_manager.get_kb_info(kbid=kbid)
        if refreshed_kb_info is None:
            logger.warning(
                "KB not found. This should not happen.", extra={"kbid": kbid}
            )
            return
        assert refreshed_kb_info.current_version == target_version

        await context.data_manager.delete_kb_migration(kbid=kbid)


async def run_all_kb_migrations(context: ExecutionContext, target_version: int) -> None:
    failures: list[str] = []
    while True:
        kbids = [
            kid
            for kid in await context.data_manager.get_kb_migrations(
                limit=1 + len(failures)
            )
            if kid not in failures
        ]
        if len(kbids) == 0:
            break

        kbid = kbids[0]
        try:
            await run_kb_migrations(context, kbid, target_version)
        except Exception as exc:
            errors.capture_exception(exc)
            logger.exception("Failed to migrate KB", extra={"kbid": kbid})
            failures.append(kbid)

    if len(failures) > 0:
        raise Exception("Failed to migrate KBs")


async def run_global_migrations(context: ExecutionContext, target_version: int) -> None:
    global_info = await context.data_manager.get_global_info()
    migrations = get_migrations(global_info.current_version, to_version=target_version)
    for migration in migrations:
        migration_info = {
            "from_version": global_info.current_version,
            "to_version": migration.version,
        }
        try:
            logger.warning("Migrating", extra=migration_info)
            with migration_observer(
                {"type": "global", "target_version": str(migration.version)}
            ):
                await migration.module.migrate(context)  # type: ignore
            await context.data_manager.update_global_info(
                current_version=migration.version
            )
            logger.warning("Finished migration", extra=migration_info)
        except Exception as exc:
            errors.capture_exception(exc)
            logger.exception("Failed to migrate", extra=migration_info)
            raise

    await context.data_manager.update_global_info(target_version=None)


async def run(context: ExecutionContext, target_version: Optional[int] = None) -> None:
    async with context.maybe_distributed_lock("migration"):
        # everything should be in a global lock
        # only 1 migration should be running at a time
        global_info = await context.data_manager.get_global_info()

        if target_version is None and global_info.target_version not in (None, 0):
            await run_all_kb_migrations(context, global_info.target_version)  # type: ignore
            await run_global_migrations(context, global_info.target_version)  # type: ignore
            global_info = await context.data_manager.get_global_info()

        migrations = get_migrations(global_info.current_version)

        if len(migrations) > 0:
            if target_version is None:
                target_version = migrations[-1].version
            # schedule all the kbs to run migrations against
            await context.data_manager.schedule_all_kbs(target_version)
            await context.data_manager.update_global_info(target_version=target_version)
            await run_all_kb_migrations(context, target_version)
            await run_global_migrations(context, target_version)


async def run_forever(context: ExecutionContext) -> None:
    """
    Most of the time this will be a noop but allows
    retrying failures until everything is done.
    """
    while True:
        try:
            await run(context)
        except Exception:
            logger.exception("Failed to run migrations. Will retry again in 5 minutes")
        await asyncio.sleep(5 * 60)
