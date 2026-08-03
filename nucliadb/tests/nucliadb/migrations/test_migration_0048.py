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
    SearchConfiguration,
)
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(48)


deprecated_models = [
    ("chatgpt-azure-5-chat", FindConfig),
    ("chatgpt-5-chat", AskConfig),
]


async def test_migration_0048(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver
    execution_context.blob_storage = Mock()

    kbid = str(uuid.uuid4())

    async with maindb_driver.rw_transaction() as txn:
        for i, (model, config_cls) in enumerate(deprecated_models):
            name = f"{model}-{i}"
            if config_cls is FindConfig:
                config: SearchConfiguration = FindSearchConfiguration(
                    kind="find", config=FindConfig(generative_model=model)
                )
            else:
                config = AskSearchConfiguration(kind="ask", config=AskConfig(generative_model=model))
            await datamanagers.search_configurations.set(txn, kbid=kbid, name=name, config=config)
        await txn.commit()

    await migration.module.migrate_kb(execution_context, kbid)

    async with maindb_driver.ro_transaction() as txn:
        for i, (model, _) in enumerate(deprecated_models):
            name = f"{model}-{i}"
            result = await datamanagers.search_configurations.get(txn, kbid=kbid, name=name)
            assert result is not None
            assert result.config.generative_model == migration.module.REPLACEMENTS[model]  # type: ignore[attr-defined]
