import logging

from .models import MigrationContext
from .context import ExecutionContext
from .utils import get_migrations

logger = logging.getLogger(__name__)


async def run_kb_migrations(
    context: ExecutionContext, kbid: str, target_version: int
) -> None:
    async with context.dist_lock_manager.lock(f"migration-{kbid}"):
        kb_info = await context.data_manager.get_kb_info(kbid)
        migrations = get_migrations(
            from_version=kb_info.current_version, to_version=target_version
        )

        for migration in migrations:
            migration_context = MigrationContext(
                from_version=kb_info.current_version,
                to_version=migration.version,
                kv_driver=context.kv_driver,
            )
            migration_info = {
                "from_version": migration_context.from_version,
                "to_version": migration_context.to_version,
                "kbid": kbid,
            }

            try:
                logger.warning("Migrating KB", extra=migration_info)
                await migration.module.kb_migrate(migration_context, kbid)  # type: ignore
                logger.warning("Finished KB Migration", extra=migration_info)
                await context.data_manager.update_kb_info(
                    kbid=kbid, current_version=migration.version
                )
            except Exception:
                logger.exception("Failed to migrate KB", extra=migration_info)
                raise
            await context.data_manager.delete_kb_migration(kbid=kbid)


async def run_all_kb_migrations(context: ExecutionContext, target_version: int) -> None:
    while True:
        kbids = await context.data_manager.get_kb_migrations(limit=1)
        if len(kbids) == 0:
            break

        await run_kb_migrations(context, kbids[0], target_version)


async def run_global_migrations(context: ExecutionContext, target_version: int) -> None:
    global_info = await context.data_manager.get_global_info()
    migrations = get_migrations(global_info.current_version, to_version=target_version)
    for migration in migrations:
        migration_context = MigrationContext(
            from_version=global_info.current_version,
            to_version=migration.version,
            kv_driver=context.kv_driver,
        )
        migration_info = {
            "from_version": migration_context.from_version,
            "to_version": migration_context.to_version,
        }
        try:
            logger.warning("Migrating", extra=migration_info)
            await migration.migrate(migration_context)  # type: ignore
            await context.data_manager.update_global_info(
                current_version=migration.version
            )
            logger.warning("Finished migration", extra=migration_info)
        except Exception:
            logger.exception("Failed to migrate", extra=migration_info)
            raise

    await context.data_manager.update_global_info(target_version=None)


async def run(context: ExecutionContext) -> None:
    async with context.dist_lock_manager.lock("migration"):
        # everything should be in a global lock
        # only 1 migration should be running at a time
        global_info = await context.data_manager.get_global_info()

        if global_info.target_version is not None:
            await run_all_kb_migrations(context, global_info.target_version)
            await run_global_migrations(context, global_info.target_version)
            global_info = await context.data_manager.get_global_info()

        migrations = get_migrations(global_info.current_version)

        if len(migrations) > 0:
            target_version = migrations[-1].version
            # schedule all the kbs to run migrations against
            await context.data_manager.schedule_all_kbs(target_version)
            await context.data_manager.update_global_info(target_version=target_version)
            await run_all_kb_migrations(context, target_version)
            await run_global_migrations(context, target_version)
