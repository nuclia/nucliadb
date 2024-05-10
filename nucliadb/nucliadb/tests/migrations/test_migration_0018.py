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

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.kb import KB_SLUGS
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.migrator.models import Migration
from nucliadb.tests.migrations import get_migration
from nucliadb_protos import knowledgebox_pb2

migration: Migration = get_migration(18)


@pytest.mark.asyncio
async def test_migration_0018_global(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver

    with (
        patch("nucliadb.ingest.orm.knowledgebox.get_storage", new=AsyncMock()),
        patch(
            "nucliadb.ingest.orm.knowledgebox.get_shard_manager",
            new=Mock(return_value=AsyncMock()),
        ),
    ):
        # setup some orphan /kbslugs keys and some real ones
        async with maindb_driver.transaction() as txn:
            fake_kb_slug = "fake-kb-slug"
            fake_kb_id = "fake-kb-id"
            key = KB_SLUGS.format(slug=fake_kb_slug)
            await txn.set(key, fake_kb_id.encode())
            assert not await datamanagers.kb.exists_kb(txn, kbid=fake_kb_id)

            real_kb_slug = "real-kb-slug"
            real_kb_id, failed = await KnowledgeBox.create(
                txn,
                slug=real_kb_slug,
                semantic_model=knowledgebox_pb2.SemanticModelMetadata(),
            )
            assert not failed
            assert await datamanagers.kb.exists_kb(txn, kbid=real_kb_id)

            await txn.commit()

        # tikv needs to open a second transaction to be able to read values from
        # the first one using `scan_keys`
        async with maindb_driver.transaction(read_only=True) as txn:
            kb_slugs = [
                kb_slug
                async for kbid, kb_slug in datamanagers.kb.get_kbs(txn, prefix="")
            ]
            assert len(kb_slugs) == 2
            assert fake_kb_slug in kb_slugs
            assert real_kb_slug in kb_slugs

    # execute migration, removing orphan kbslug keys
    await migration.module.migrate(execution_context)

    async with maindb_driver.transaction(read_only=True) as txn:
        assert not await datamanagers.kb.exists_kb(txn, kbid=fake_kb_id)
        assert await datamanagers.kb.exists_kb(txn, kbid=real_kb_id)

        value = await txn.get(KB_SLUGS.format(slug=fake_kb_slug))
        assert value is None

        value = await txn.get(KB_SLUGS.format(slug=real_kb_slug))
        assert value is not None
        assert value.decode() == real_kb_id

        kb_slugs = [
            kb_slug async for kbid, kb_slug in datamanagers.kb.get_kbs(txn, prefix="")
        ]
        assert len(kb_slugs) == 1
        assert real_kb_slug in kb_slugs
