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
from unittest.mock import Mock

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.migrator.models import Migration
from nucliadb.migrator.utils import get_migrations
from nucliadb_protos import writer_pb2

MIGRATION_UNDER_TEST = 17
migration: Migration = get_migrations(
    from_version=MIGRATION_UNDER_TEST - 1, to_version=MIGRATION_UNDER_TEST
)[0]
assert migration.version == MIGRATION_UNDER_TEST


@pytest.mark.asyncio
async def test_migration_0017_kb(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    # setup a KB with the old format
    kbid = "my-kbid"
    async with maindb_driver.transaction() as txn:
        shards = writer_pb2.Shards(
            kbid=kbid,
            shards=[
                writer_pb2.ShardObject(
                    shard="shard-0",
                    replicas=[
                        writer_pb2.ShardReplica(),
                        writer_pb2.ShardReplica(),
                    ],
                ),
                writer_pb2.ShardObject(
                    shard="shard-1",
                    replicas=[
                        writer_pb2.ShardReplica(),
                        writer_pb2.ShardReplica(),
                    ],
                ),
            ],
            # this field is an index to shards list indicating which is the
            # writable shard
            actual=1,
        )
        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=shards)
        await txn.commit()

    await migration.module.migrate_kb(execution_context, kbid)

    async with maindb_driver.transaction(read_only=True) as txn:
        migrated = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        assert migrated is not None

        # actual is the same
        assert shards.actual == 1

        # Writable shard now is pointed by `ShardObject.read_only` field
        assert [True, False] == [shard.read_only for shard in migrated.shards]
