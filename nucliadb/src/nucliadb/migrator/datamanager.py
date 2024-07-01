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
from typing import Optional, Union

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import (
    KnowledgeBox as KnowledgeBoxORM,  # probably bad that we are using this here but I didn't want to copy the logic
)
from nucliadb_protos import migrations_pb2

from .models import GlobalInfo, KnowledgeBoxInfo


class _Unset:
    pass


_UNSET = _Unset()

MIGRATIONS_CONTAINER_KEY = "migrations/"
MIGRATIONS_KEY = "migrations/{kbid}"
MIGRATION_INFO_KEY = "migration/info"
ROLLOVER_CONTAINER_KEY = "rollover/"
ROLLOVER_KEY = "rollover/{kbid}"


class MigrationsDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def schedule_all_kbs(self, target_version: int) -> None:
        # Get all kb ids
        async with self.driver.transaction(read_only=True) as txn:
            kbids = [kbid async for kbid, _ in datamanagers.kb.get_kbs(txn)]
        # Schedule the migrations
        async with self.driver.transaction() as txn:
            for kbid in kbids:
                await txn.set(MIGRATIONS_KEY.format(kbid=kbid), str(target_version).encode())
            await txn.commit()

    async def get_kb_migrations(self, limit: int = 100) -> list[str]:
        keys = []
        async with self.driver.transaction() as txn:
            async for key in txn.keys(MIGRATIONS_CONTAINER_KEY, count=limit):
                keys.append(key.split("/")[-1])

        return keys

    async def delete_kb_migration(self, *, kbid: str) -> None:
        async with self.driver.transaction() as txn:
            await txn.delete(MIGRATIONS_KEY.format(kbid=kbid))
            await txn.commit()

    async def get_kb_info(self, kbid: str) -> Optional[KnowledgeBoxInfo]:
        async with self.driver.transaction(read_only=True) as txn:
            kb_config = await datamanagers.kb.get_config(txn, kbid=kbid)
            if kb_config is None:
                return None
        return KnowledgeBoxInfo(current_version=kb_config.migration_version)

    async def update_kb_info(self, *, kbid: str, current_version: int) -> None:
        async with self.driver.transaction() as txn:
            kb_config = await datamanagers.kb.get_config(txn, kbid=kbid, for_update=True)
            if kb_config is None:
                raise Exception(f"KB {kbid} does not exist")
            kb_config.migration_version = current_version
            await KnowledgeBoxORM.update(txn, kbid, config=kb_config)
            await txn.commit()

    async def get_global_info(self) -> GlobalInfo:
        async with self.driver.transaction(read_only=True) as txn:
            raw_pb = await txn.get(MIGRATION_INFO_KEY)
        if raw_pb is None:
            return GlobalInfo(current_version=0, target_version=None)
        pb = migrations_pb2.MigrationInfo()
        pb.ParseFromString(raw_pb)
        return GlobalInfo(current_version=pb.current_version, target_version=pb.target_version)

    async def update_global_info(
        self,
        *,
        current_version: Union[int, _Unset] = _UNSET,
        target_version: Union[int, None, _Unset] = _UNSET,
    ) -> None:
        async with self.driver.transaction() as txn:
            raw_pb = await txn.get(MIGRATION_INFO_KEY, for_update=True)
            pb = migrations_pb2.MigrationInfo()
            if raw_pb is not None:
                pb.ParseFromString(raw_pb)
            if not isinstance(current_version, _Unset):
                pb.current_version = current_version
            if not isinstance(target_version, _Unset):
                if target_version is None:
                    pb.ClearField("target_version")
            await txn.set(MIGRATION_INFO_KEY, pb.SerializeToString())

            await txn.commit()

    async def get_kbs_to_rollover(self) -> list[str]:
        keys = []
        async with self.driver.transaction() as txn:
            async for key in txn.keys(ROLLOVER_CONTAINER_KEY):
                keys.append(key.split("/")[-1])

        return keys

    async def add_kb_rollover(self, kbid: str) -> None:
        async with self.driver.transaction() as txn:
            await txn.set(ROLLOVER_KEY.format(kbid=kbid), b"")
            await txn.commit()

    async def delete_kb_rollover(self, kbid: str) -> None:
        async with self.driver.transaction() as txn:
            await txn.delete(ROLLOVER_KEY.format(kbid=kbid))
            await txn.commit()
