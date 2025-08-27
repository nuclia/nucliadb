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

migration: Migration = get_migration(30)


async def test_migration_0030_duplicated_labels(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    kbid = str(uuid.uuid4())

    kb_labels = knowledgebox_pb2.Labels()
    for i in range(2):
        kb_labels.labelset["labelset"].labels.append(knowledgebox_pb2.Label(title="my duplicated label"))

    kb_labels.labelset["another"].labels.append(knowledgebox_pb2.Label(title="another label"))

    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.labels.set_labels(txn, kbid=kbid, labels=kb_labels)
        await txn.commit()

    await migration.module.migrate_kb(execution_context, kbid)

    async with maindb_driver.ro_transaction() as txn:
        dedup_labels = await datamanagers.labels.get_labels(txn, kbid=kbid)
        assert len(dedup_labels.labelset["labelset"].labels) == 1
        assert dedup_labels.labelset["labelset"].labels[0].title == "my duplicated label"
