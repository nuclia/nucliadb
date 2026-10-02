"""Runner for MarkLogic schema migrations."""

from __future__ import annotations

import importlib
import os
from types import ModuleType

from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.marklogic import MarkLogicDriver

MIGRATION_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "migrations", "marklogic")
LEDGER_PREFIX = "/internal/migrations/marklogic/"


def get_marklogic_migrations() -> list[tuple[int, ModuleType]]:
    migrations: list[tuple[int, ModuleType]] = []
    for filename in os.listdir(MIGRATION_DIR):
        if not filename.endswith(".py") or filename == "__init__.py":
            continue
        module_name = filename[:-3]
        version = int(module_name.split("_")[0])
        module = importlib.import_module(f"migrations.marklogic.{module_name}")
        if not hasattr(module, "migrate"):
            raise RuntimeError(f"Missing migrate() in MarkLogic migration {module_name}")
        migrations.append((version, module))
    return sorted(migrations)


async def run_marklogic_schema_migrations(driver: Driver) -> None:
    if not isinstance(driver, MarkLogicDriver):
        raise RuntimeError("MarkLogic schema migrations require MarkLogicDriver")

    await driver.ensure_system_database()
    for version, migration in get_marklogic_migrations():
        key = f"{LEDGER_PREFIX}{version}"
        async with driver.ro_transaction() as transaction:
            if await transaction.get(key) is not None:
                continue
        await migration.migrate(driver)
        async with driver.rw_transaction() as transaction:
            await transaction.set(key, migration.__name__.encode())
            await transaction.commit()
