# nucliadb

This module contains most of the Python components for NucliaDB:

- ingest
- reader
- writer
- search
- train

# NucliaDB Migrations

This module is used to manage NucliaDB Migrations.

All migrations will be provided in the `migrations` folder and have a filename
that follows the structure: `[sequence]_[migration name].py`.
Where `sequence` is the order the migration should be run in with zero padding.
Example: `0001_migrate_data.py`.

Each migration should have the following:

```python
from nucliadb.migrator.context import ExecutionContext


async def migrate(context: ExecutionContext) -> None:
    """
    Non-kb type of migration. Migrate global data.
    """


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
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

- Migrations are currently run with a deployment and will be continuously retried on failure.
- Running migrations in a deployment is to make sure a migration does not prevent code deployment.