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

from nucliadb.common import datamanagers as dm
from nucliadb.common.maindb.driver import Driver
from nucliadb.migrator.models import Migration
from nucliadb_protos import knowledgebox_pb2, resources_pb2, writer_pb2
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(50)


async def test_migration_0050_backfill_kb_runs_smoothly(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver
    execution_context.blob_storage = Mock()

    kbid = str(uuid.uuid4())
    shard_id = "shard-001"

    resources = [
        (uuid.uuid4().hex, "resource-1"),
        (uuid.uuid4().hex, "resource-2"),
        (uuid.uuid4().hex, "resource-3"),
    ]

    config = knowledgebox_pb2.KnowledgeBoxConfig(slug="kb-0050-test")
    shards = writer_pb2.Shards(actual=0)
    shard = shards.shards.add()
    shard.shard = shard_id
    shard.nidx_shard_id = "nidx-shard-001"

    async with maindb_driver.rw_transaction() as txn:
        await txn.set(migration.module.kbs_v1.KB_UUID.format(kbid=kbid), config.SerializeToString())
        await txn.set(
            migration.module.cluster_v1.KB_SHARDS.format(kbid=kbid), shards.SerializeToString()
        )

        for rid, slug in resources:
            basic = resources_pb2.Basic(slug=slug)
            await txn.set(
                migration.module.resources_v1.KB_RESOURCE_BASIC.format(kbid=kbid, uuid=rid),
                basic.SerializeToString(),
            )
            await txn.set(
                migration.module.resources_v1.KB_RESOURCE_SHARD.format(kbid=kbid, uuid=rid),
                shard_id.encode(),
            )
            await txn.set(
                migration.module.resources_v1.KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug),
                rid.encode(),
            )

        await txn.commit()

    # The test goal is coverage: this should complete without raising exceptions.
    await migration.module.migrate_kb(execution_context, kbid)

    async with dm.with_ro_transaction() as txn:
        assert await dm.kb.exists_kb(txn, kbid=kbid) is True
        assert await dm.cluster.get_kb_shards(txn, kbid=kbid) == shards
        assert await dm.kb.get_config(txn, kbid=kbid) == config
        for rid, slug in resources:
            basic = resources_pb2.Basic(slug=slug)
            assert await dm.resources.get_basic(txn, kbid=kbid, rid=rid) == basic
            assert await dm.resources.get_resource_shard_id(txn, kbid=kbid, rid=rid) == shard_id
            assert await dm.resources.get_resource_uuid_from_slug(txn, kbid=kbid, slug=slug) == rid
