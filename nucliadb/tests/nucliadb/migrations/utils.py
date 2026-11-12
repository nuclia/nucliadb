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
# Copyright (C) 2021 Bosutech XXI S.L.

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
from tests.ndbfixtures.utils import application_context


async def _test_model_deprecation_migration(
    maindb_driver: Driver,
    migration: Migration,
    *,
    deprecated_model: str,
    replacement_model: str,
):
    context = application_context(
        maindb_driver=maindb_driver,
        storage=Mock(),
    )
    kbid = str(uuid.uuid4())

    async with maindb_driver.rw_transaction() as txn:
        find: SearchConfiguration = FindSearchConfiguration(
            kind="find", config=FindConfig(generative_model=deprecated_model)
        )
        await datamanagers.search_configurations.set(txn, kbid=kbid, name="find", config=find)
        ask: SearchConfiguration = AskSearchConfiguration(
            kind="ask", config=AskConfig(generative_model=deprecated_model)
        )
        await datamanagers.search_configurations.set(txn, kbid=kbid, name="ask", config=ask)

        find_custom: SearchConfiguration = FindSearchConfiguration(
            kind="find", config=FindConfig(generative_model=f"{deprecated_model}/custom")
        )
        await datamanagers.search_configurations.set(
            txn, kbid=kbid, name="find-custom", config=find_custom
        )
        ask_custom: SearchConfiguration = AskSearchConfiguration(
            kind="ask", config=AskConfig(generative_model=f"{deprecated_model}/custom")
        )
        await datamanagers.search_configurations.set(
            txn, kbid=kbid, name="ask-custom", config=ask_custom
        )

        await txn.commit()

    await migration.module.migrate_kb(context, kbid)

    async with maindb_driver.ro_transaction() as txn:
        for name in ("find", "ask"):
            migrated = await datamanagers.search_configurations.get(txn, kbid=kbid, name=name)
            assert migrated is not None
            assert migrated.config.generative_model == replacement_model  # type: ignore[attr-defined]

        for name in ("find-custom", "ask-custom"):
            preserved = await datamanagers.search_configurations.get(txn, kbid=kbid, name=name)
            assert preserved is not None
            assert preserved.config.generative_model == f"{deprecated_model}/custom"  # type: ignore[attr-defined]
