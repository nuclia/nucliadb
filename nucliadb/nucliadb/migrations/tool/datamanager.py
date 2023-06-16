from typing import Union

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import (
    KnowledgeBox as KnowledgeBoxORM,  # probably bad that we are using this here but I didn't want to copy the logic
)
from nucliadb_protos import migrations_pb2

from .models import GlobalInfo, KnowledgeBoxInfo


class _Unset:
    pass


_UNSET = _Unset()

MIGRATIONS_KEY = "migrations/{kbid}"
MIGRATION_INFO = "migration/info"


class MigrationsDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def schedule_all_kbs(self, target_version: int) -> None:
        async with self.driver.transaction() as txn:
            async for kbid, _ in KnowledgeBoxORM.get_kbs(txn, slug=""):
                await txn.set(
                    MIGRATIONS_KEY.format(kbid=kbid), str(target_version).encode()
                )
            await txn.commit()

    async def get_kb_migrations(self, limit: int = 100) -> list[str]:
        keys = []
        async with self.driver.transaction() as txn:
            async for key in txn.keys("migrations/", count=limit):
                keys.append(key.split("/")[-1])

        return keys

    async def delete_kb_migration(self, *, kbid: str) -> None:
        async with self.driver.transaction() as txn:
            await txn.delete(MIGRATIONS_KEY.format(kbid=kbid))
            await txn.commit()

    async def get_kb_info(self, kbid: str) -> KnowledgeBoxInfo:
        async with self.driver.transaction() as txn:
            kb_config = await KnowledgeBoxORM.get_kb(txn, kbid)
            if kb_config is None:
                raise Exception(f"KB {kbid} does not exist")
        return KnowledgeBoxInfo(current_version=kb_config.migration_version)

    async def update_kb_info(self, *, kbid: str, current_version: int) -> None:
        async with self.driver.transaction() as txn:
            kb_config = await KnowledgeBoxORM.get_kb(txn, kbid)
            if kb_config is None:
                raise Exception(f"KB {kbid} does not exist")
            kb_config.migration_version = current_version
            await KnowledgeBoxORM.update(txn, kbid, config=kb_config)
            await txn.commit()

    async def get_global_info(self) -> GlobalInfo:
        async with self.driver.transaction() as txn:
            raw_pb = await txn.get(MIGRATION_INFO)
        if raw_pb is None:
            return GlobalInfo(current_version=0, target_version=None)
        pb = migrations_pb2.MigrationInfo()
        pb.ParseFromString(raw_pb)
        return GlobalInfo(
            current_version=pb.current_version, target_version=pb.target_version
        )

    async def update_global_info(
        self,
        *,
        current_version: Union[int, _Unset] = _UNSET,
        target_version: Union[int, None, _Unset] = _UNSET,
    ) -> None:
        async with self.driver.transaction() as txn:
            raw_pb = await txn.get(MIGRATION_INFO)
            pb = migrations_pb2.MigrationInfo()
            if raw_pb is not None:
                pb.ParseFromString(raw_pb)
            if not isinstance(current_version, _Unset):
                pb.current_version = current_version
            if not isinstance(target_version, _Unset):
                if target_version is None:
                    pb.ClearField("target_version")
            await txn.set(MIGRATION_INFO, pb.SerializeToString())

            await txn.commit()
