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
from nucliadb_models.configuration import (
    AskConfig,
    AskSearchConfiguration,
    FindConfig,
    FindSearchConfiguration,
)
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(40)


async def test_migration_0040(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver
    storage = Mock()
    execution_context.blob_storage = storage

    kbid = str(uuid.uuid4())

    # Create a couple of search configurations
    async with maindb_driver.rw_transaction() as txn:
        find = FindSearchConfiguration(
            kind="find", config=FindConfig(generative_model="claude-3-5-small")
        )
        await datamanagers.search_configurations.set(txn, kbid=kbid, name="find", config=find)

        ask = AskSearchConfiguration(
            kind="ask", config=AskConfig(generative_model="gcp-claude-3-5-sonnet-v2")
        )
        await datamanagers.search_configurations.set(txn, kbid=kbid, name="ask", config=ask)

        await txn.commit()

    await migration.module.migrate_kb(execution_context, kbid)

    # Make sure that generative_models have been replaced
    async with maindb_driver.ro_transaction() as txn:
        find_ = await datamanagers.search_configurations.get(txn, kbid=kbid, name="find")
        assert find_ is not None
        assert find_.config.generative_model == "claude-4-5-sonnet"  # type: ignore

        ask_ = await datamanagers.search_configurations.get(txn, kbid=kbid, name="ask")
        assert ask_ is not None
        assert ask_.config.generative_model == "gcp-claude-4-5-sonnet"  # type: ignore
