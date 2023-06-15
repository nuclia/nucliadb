import logging
import os

import nucliadb.migrations

from .models import Migration
from functools import lru_cache

logger = logging.getLogger(__name__)

MIGRATION_DIR = os.path.sep.join(
    os.path.dirname(os.path.abspath(__file__)).split(os.path.sep)[:-1]
)


def get_migrations(from_version: int = 0, to_version: int = 99999999):
    migrations: list[Migration] = []
    for filename in os.listdir(MIGRATION_DIR):
        if filename.endswith(".py") and filename != "__init__.py":
            module_name = filename[:-3]
            version = int(module_name.split("_")[-1])
            __import__(f"nucliadb.migrations.{module_name}")
            module = getattr(nucliadb.migrations, module_name)
            if not hasattr(module, "migrate"):
                raise Exception(f"Missing `migrate` function in {module_name}")
            if not hasattr(module, "migrate_kb"):
                raise Exception(f"Missing `migrate_kb` function in {module_name}")
            migrations.append(Migration(version=version, module=module))

    migrations.sort(key=lambda m: m.version)
    return [
        m for m in migrations if m.version > from_version and m.version <= to_version
    ]


@lru_cache(maxsize=None)
def get_latest_version() -> int:
    return get_migrations()[-1].version
