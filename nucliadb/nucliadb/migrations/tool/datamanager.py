from typing import AsyncIterator, Optional
from .models import GlobalInfo, KnowledgeBoxInfo
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM

_UNSET = object()

MIGRATIONS_KEY = "migrations/"


class MigrationsDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def schedule_all_kbs(self, target_version: int) -> None:
        async with self.driver.transaction() as txn:
            async for uuid, slug in KnowledgeBoxORM.get_kbs(txn):
                ...

    async def get_kb_migrations(self, limit: int = 100) -> list[str]:
        ...

    async def delete_kb_migration(self, *, kbid: str) -> None:
        ...

    async def get_kb_info(self) -> KnowledgeBoxInfo:
        ...

    async def update_kb_info(self, *, kbid: str, current_version: str) -> None:
        ...

    async def get_global_info(self) -> GlobalInfo:
        ...

    async def update_global_info(
        self, *, current_version: str = _UNSET, target_version: str = _UNSET
    ) -> None:
        ...
