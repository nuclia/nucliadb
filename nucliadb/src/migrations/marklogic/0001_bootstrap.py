"""Create and configure the shared MarkLogic system database.

Per-KnowledgeBox databases are provisioned on KnowledgeBox creation, not here.
"""

from __future__ import annotations

from nucliadb.common.maindb.marklogic import MarkLogicDriver


async def migrate(driver: MarkLogicDriver) -> None:
    await driver.ensure_system_database()
