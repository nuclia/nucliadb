from .tool import MigrationContext


async def migrate(context: MigrationContext) -> None:
    """
    Non-kb type of migration.
    """


async def migrate_kb(context: MigrationContext, kbid: str) -> None:
    """
    Migrate kb.

    Must have both types of migrations.
    """
