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

import pytest

from nucliadb.common.maindb.driver import Driver
from nucliadb.migrator.models import Migration
from nucliadb.tests.migrations import get_migration

migration: Migration = get_migration(21)


@pytest.mark.asyncio
async def test_migration_0021(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    key = "/kbs/{kbid}/vectorsets"

    kv = {}
    for _ in range(200):
        kbid = str(uuid.uuid4())
        kv[f"/kbs/{kbid}"] = b"my kb"
        kv[f"/kbs/{kbid}/vectorsets"] = b"vectorset data"
        kv[f"/kbs/{kbid}/other"] = b"other data"

    # Create a bunch of deprecated vectorsets keys and add
    async with maindb_driver.transaction() as txn:
        for key, value in kv.items():
            await txn.set(key, value)
        await txn.commit()

    async with maindb_driver.transaction(read_only=True) as txn:
        for key, value in kv.items():
            stored = await txn.get(key)
            assert stored == value

    await migration.module.migrate(execution_context)

    async with maindb_driver.transaction(read_only=True) as txn:
        for key, value in kv.items():
            stored = await txn.get(key)
            if key.endswith("/vectorsets"):
                assert stored is None
            else:
                assert stored == value
