# NucliaDB Migrations

This module is used to manage NucliaDB Migrations.

All migrations will be provided in this folder have a filename
that follows the structure: `[migration name]_[sequence].py`.
Where `sequence` is the order the migration should be run in. Example: `migrate_data_1.py`.

Each migration should have the following:

```python
from .tool import MigrationContext


async def migrate(context: MigrationContext) -> None:
    """
    Non-kb type of migration. Migrate global data.
    """


async def migrate_kb(context: MigrationContext, kbid: str) -> None:
    """
    Migrate kb.

    Must have both types of migrations.
    """
```


## How migrations are managed

- All migrations utilize a distributed lock to prevent simulateously running jobs
- Global migration state:
    - current version
    - target version
    - KBs to migrate
- KB Migration State:
    - current version