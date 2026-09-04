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

from nucliadb.common.maindb.driver import Driver
from nucliadb.migrator.models import Migration
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(51)


async def test_migration_0051_removes_old_kv_keys(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    kbid = str(uuid.uuid4())
    rid = uuid.uuid4().hex
    slug = "my-resource-slug"
    kb_slug = "my-kb-slug"

    old_keys = [
        f"/kbs/{kbid}/r/{rid}",
        f"/kbs/{kbid}/r/{rid}/basic",
        f"/kbs/{kbid}/r/{rid}/origin",
        f"/kbs/{kbid}/r/{rid}/extra",
        f"/kbs/{kbid}/r/{rid}/security",
        f"/kbs/{kbid}/r/{rid}/shard",
        f"/kbs/{kbid}/r/{rid}/allfields",
        f"/kbs/{kbid}/r/{rid}/f/t/my-text-field",
        f"/kbs/{kbid}/r/{rid}/f/t/my-text-field/error",
        f"/kbs/{kbid}/r/{rid}/f/t/my-text-field/status",
        f"/kbs/{kbid}/r/{rid}/f/c/my-conv-field/1",
        f"/kbs/{kbid}/r/{rid}/f/c/my-conv-field/splits_metadata",
        f"/kbs/{kbid}/r/{rid}/u/t/my-text-field",
        f"/kbs/{kbid}/s/{slug}",
        f"/kbslugs/{kb_slug}",
        f"/kbs/{kbid}/materialized/resources/count",
        f"/kbs/{kbid}/config",
    ]
    keys_to_keep = [
        f"/kbs/{kbid}",
        f"/kbs/{kbid}/labels",
        f"/kbs/{kbid}/vectorsets",
        f"/kbs/{kbid}/rollover/state",
        f"/kbs/{kbid}/search_configuration/my-config",
    ]

    async with maindb_driver.rw_transaction() as txn:
        for key in old_keys + keys_to_keep:
            await txn.set(key, b"some data")
        await txn.commit()

    await migration.module.migrate(execution_context)

    async with maindb_driver.ro_transaction() as txn:
        for key in old_keys:
            assert await txn.get(key) is None
        for key in keys_to_keep:
            assert await txn.get(key) == b"some data"
