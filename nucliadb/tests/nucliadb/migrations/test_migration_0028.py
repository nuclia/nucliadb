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
import uuid
from unittest.mock import Mock

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.migrator.models import Migration
from nucliadb_protos import knowledgebox_pb2
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(28)


async def test_migration_0028_single_vectorset_kb(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    kbid = str(uuid.uuid4())
    config = knowledgebox_pb2.VectorSetConfig(
        vectorset_id="single-1",
        storage_key_kind=knowledgebox_pb2.VectorSetConfig.StorageKeyKind.UNSET,
    )

    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.vectorsets.set(txn, kbid=kbid, config=config)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert (await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="single-1")) == config

    await migration.module.migrate_kb(execution_context, kbid)

    async with maindb_driver.ro_transaction() as txn:
        vs = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="single-1")
        assert vs is not None
        assert vs.vectorset_id == "single-1"
        assert vs.storage_key_kind == knowledgebox_pb2.VectorSetConfig.StorageKeyKind.LEGACY


async def test_migration_0028_multi_vectorset_kb(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    kbid = str(uuid.uuid4())
    config_1 = knowledgebox_pb2.VectorSetConfig(
        vectorset_id="multi-1",
        storage_key_kind=knowledgebox_pb2.VectorSetConfig.StorageKeyKind.UNSET,
    )
    config_2 = knowledgebox_pb2.VectorSetConfig(
        vectorset_id="multi-2",
        storage_key_kind=knowledgebox_pb2.VectorSetConfig.StorageKeyKind.UNSET,
    )

    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.vectorsets.set(txn, kbid=kbid, config=config_1)
        await datamanagers.vectorsets.set(txn, kbid=kbid, config=config_2)
        await txn.commit()

    async with maindb_driver.ro_transaction() as txn:
        assert (await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="multi-1")) == config_1
        assert (await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="multi-2")) == config_2

    await migration.module.migrate_kb(execution_context, kbid)

    async with maindb_driver.ro_transaction() as txn:
        vs = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="multi-1")
        assert vs is not None
        assert vs.vectorset_id == "multi-1"
        assert vs.storage_key_kind == knowledgebox_pb2.VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX

        vs = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id="multi-2")
        assert vs is not None
        assert vs.vectorset_id == "multi-2"
        assert vs.storage_key_kind == knowledgebox_pb2.VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX
