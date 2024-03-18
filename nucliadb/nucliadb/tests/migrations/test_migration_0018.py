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
from unittest.mock import AsyncMock, Mock, patch

import pytest

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KB_SLUGS, KnowledgeBox
from nucliadb.migrator.models import Migration
from nucliadb.migrator.utils import get_migrations

MIGRATION_UNDER_TEST = 18
migration: Migration = get_migrations(
    from_version=MIGRATION_UNDER_TEST - 1, to_version=MIGRATION_UNDER_TEST
)[0]
assert migration.version == MIGRATION_UNDER_TEST


@pytest.mark.asyncio
async def test_migration_0017_global(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    with patch("nucliadb.ingest.orm.knowledgebox.get_storage", new=AsyncMock()), patch(
        "nucliadb.ingest.orm.knowledgebox.get_shard_manager",
        new=Mock(return_value=AsyncMock()),
    ):
        # setup some orphan /kbslugs keys and some real ones
        async with maindb_driver.transaction() as txn:
            fake_kb_slug = "fake-kb-slug"
            fake_kb_id = "fake-kb-id"
            key = KB_SLUGS.format(slug=fake_kb_slug)
            await txn.set(key, fake_kb_id.encode())
            assert not await KnowledgeBox.exist_kb(txn, fake_kb_id)

            real_kb_slug = "real-kb-slug"
            real_kb_id, failed = await KnowledgeBox.create(
                txn, slug=real_kb_slug, semantic_model=Mock()
            )
            assert not failed
            assert await KnowledgeBox.exist_kb(txn, real_kb_id)

            kb_slugs = [
                kb_slug async for kbid, kb_slug in KnowledgeBox.get_kbs(txn, slug="")
            ]
            assert len(kb_slugs) == 2
            assert fake_kb_slug in kb_slugs
            assert real_kb_slug in kb_slugs

            await txn.commit()

    # execute migration, removing orphan kbslug keys
    await migration.module.migrate(execution_context)

    async with maindb_driver.transaction(read_only=True) as txn:
        assert not await KnowledgeBox.exist_kb(txn, fake_kb_id)
        assert await KnowledgeBox.exist_kb(txn, real_kb_id)

        value = await txn.get(KB_SLUGS.format(slug=fake_kb_slug))
        assert value is None

        value = await txn.get(KB_SLUGS.format(slug=real_kb_slug))
        assert value is not None
        assert value.decode() == real_kb_id

        kb_slugs = [
            kb_slug async for kbid, kb_slug in KnowledgeBox.get_kbs(txn, slug="")
        ]
        assert len(kb_slugs) == 1
        assert real_kb_slug in kb_slugs
