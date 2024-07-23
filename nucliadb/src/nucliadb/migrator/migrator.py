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

from nucliadb.common import locking
from nucliadb.common.cluster.rollover import rollover_kb_shards
from nucliadb.common.cluster.settings import in_standalone_mode
from nucliadb.common.maindb.pg import PGDriver
from nucliadb.migrator.context import ExecutionContext
from nucliadb.migrator.utils import get_migrations, get_pg_migrations
from nucliadb_telemetry import errors, metrics

migration_observer = metrics.Observer("nucliadb_migrations", labels={"type": "kb", "target_version": ""})


logger = logging.getLogger(__name__)


async def run_kb_migrations(context: ExecutionContext, kbid: str, target_version: int) -> None:
    async with locking.distributed_lock(f"migration-{kbid}"):
        kb_info = await context.data_manager.get_kb_info(kbid)
        if kb_info is None:
            logger.warning("KB not found", extra={"kbid": kbid})
            await context.data_manager.delete_kb_migration(kbid=kbid)
            return

        migrations = get_migrations(from_version=kb_info.current_version, to_version=target_version)

        for migration in migrations:
            migration_info = {
                "from_version": kb_info.current_version,
                "to_version": migration.version,
                "kbid": kbid,
            }

            try:
                logger.info("Migrating KB", extra=migration_info)
                with migration_observer({"type": "kb", "target_version": str(migration.version)}):
                    await migration.module.migrate_kb(context, kbid)
                logger.info("Finished KB Migration", extra=migration_info)
                await context.data_manager.update_kb_info(kbid=kbid, current_version=migration.version)
            except Exception as exc:
                errors.capture_exception(exc)
                logger.exception("Failed to migrate KB", extra=migration_info)
                raise

        refreshed_kb_info = await context.data_manager.get_kb_info(kbid=kbid)
        if refreshed_kb_info is None:
            logger.warning("KB not found. This should not happen.", extra={"kbid": kbid})
            return
        assert refreshed_kb_info.current_version == target_version

        await context.data_manager.delete_kb_migration(kbid=kbid)


async def run_all_kb_migrations(context: ExecutionContext, target_version: int) -> None:
    """
    Schedule all KB migrations to run in parallel. Only a certain number of migrations will run at the same time.
    If any of the migrations fail, the whole process will fail.
    """
    to_migrate = await context.data_manager.get_kb_migrations(limit=-1)

    if len(to_migrate) == 0:
        return
    if in_standalone_mode():
        max_concurrent = 1
    else:
        max_concurrent = context.settings.max_concurrent_migrations
    semaphore = asyncio.Semaphore(max_concurrent)

    logger.info(
        f"Scheduling {len(to_migrate)} KB migrations. Max concurrent: {max_concurrent}",
        extra={"target_version": target_version},
    )

    tasks = [
        asyncio.create_task(
            run_kb_migrations_in_parallel(context, kbid, target_version, semaphore),
            name=f"migrate_kb_{kbid}",
        )
        for kbid in to_migrate
    ]

    failures = 0
    for future in asyncio.as_completed(tasks):
        try:
            await future
        except Exception:
            failures += 1

    if failures > 0:
        raise Exception(f"Failed to migrate KBs. Failures: {failures}")


async def run_kb_migrations_in_parallel(
    context: ExecutionContext,
    kbid: str,
    target_version: int,
    max_concurrent: asyncio.Semaphore,
) -> None:
    async with max_concurrent:
        try:
            await run_kb_migrations(context, kbid, target_version)
            logger.info(
                f"Finished KB migration",
                extra={"kbid": kbid, "target_version": target_version},
            )
        except Exception as exc:
            errors.capture_exception(exc)
            logger.exception(
                "Failed to migrate KB",
                extra={"kbid": kbid, "target_version": target_version},
            )
            raise


async def run_global_migrations(context: ExecutionContext, target_version: int) -> None:
    global_info = await context.data_manager.get_global_info()
    migrations = get_migrations(global_info.current_version, to_version=target_version)
    for migration in migrations:
        migration_info = {
            "from_version": global_info.current_version,
            "to_version": migration.version,
        }
        try:
            logger.info("Migrating", extra=migration_info)
            with migration_observer({"type": "global", "target_version": str(migration.version)}):
                await migration.module.migrate(context)
            await context.data_manager.update_global_info(current_version=migration.version)
            logger.info("Finished migration", extra=migration_info)
        except Exception as exc:
            errors.capture_exception(exc)
            logger.exception("Failed to migrate", extra=migration_info)
            raise

    await context.data_manager.update_global_info(target_version=None)


async def run_rollover_in_parallel(
    context: ExecutionContext,
    kbid: str,
    max_concurrent: asyncio.Semaphore,
) -> None:
    async with max_concurrent:
        try:
            await rollover_kb_shards(context, kbid)
            await context.data_manager.delete_kb_rollover(kbid=kbid)
        except Exception as exc:
            errors.capture_exception(exc)
            logger.exception(
                "Failed to rollover KB",
                extra={"kbid": kbid},
            )
            raise


async def run_rollovers(context: ExecutionContext) -> None:
    kbs_to_rollover = await context.data_manager.get_kbs_to_rollover()

    if len(kbs_to_rollover) == 0:
        return

    max_concurrent = context.settings.max_concurrent_migrations
    semaphore = asyncio.Semaphore(max_concurrent)

    logger.info(
        f"Scheduling KB rollovers.",
        extra={"max_concurrent": max_concurrent, "kbs": len(kbs_to_rollover)},
    )

    tasks = [
        asyncio.create_task(
            run_rollover_in_parallel(context, kbid, semaphore),
            name=f"rollover_kb_{kbid}",
        )
        for kbid in kbs_to_rollover
    ]

    failures = 0
    for future in asyncio.as_completed(tasks):
        try:
            await future
        except Exception:
            failures += 1

    if failures > 0:
        raise Exception(f"Failed to migrate KBs. Failures: {failures}")


async def run_pg_schema_migrations(driver: PGDriver):
    migrations = get_pg_migrations()

    # The migration uses two transactions. The former is only used to get a lock (pg_advisory_lock)
    # without having to worry about correctly unlocking it (postgres unlocks it when the transaction ends)
    async with driver.transaction() as tx_lock, tx_lock.connection.cursor() as cur_lock:  # type: ignore[attr-defined]
        await cur_lock.execute(
            "CREATE TABLE IF NOT EXISTS migrations (version INT PRIMARY KEY, migrated_at TIMESTAMP NOT NULL DEFAULT NOW())"
        )
        await tx_lock.commit()
        await cur_lock.execute("SELECT pg_advisory_xact_lock(3116614845278015934)")

        await cur_lock.execute("SELECT version FROM migrations")
        migrated = [r[0] for r in await cur_lock.fetchall()]

        for version, migration in migrations:
            if version in migrated:
                continue

            # Gets a new transaction for each migration, so if they get interrupted we at least
            # save the state of the last finished transaction
            async with driver.transaction() as tx, tx.connection.cursor() as cur:  # type: ignore[attr-defined]
                await migration.migrate(tx)
                await cur.execute("INSERT INTO migrations (version) VALUES (%s)", (version,))
                await tx.commit()


async def run(context: ExecutionContext, target_version: Optional[int] = None) -> None:
    # Run schema migrations first, since they create the `resources` table needed for the lock below
    # Schema migrations use their own locking system
    if isinstance(context.kv_driver, PGDriver):
        await run_pg_schema_migrations(context.kv_driver)

    async with locking.distributed_lock(locking.MIGRATIONS_LOCK):
        # before we move to managed migrations, see if there are any rollovers
        # scheduled and run them
        await run_rollovers(context)

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
