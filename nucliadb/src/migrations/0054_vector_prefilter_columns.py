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

"""Add vector TDE prefilter columns; MarkLogic reindexes existing embeddings asynchronously."""

from nucliadb.common import datamanagers
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.marklogic import vector_views
from nucliadb.migrator.context import ExecutionContext


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    driver = context.kv_driver
    if not isinstance(driver, MarkLogicDriver):
        return

    async with driver.ro_transaction(kbid=kbid) as txn:
        vectorsets = [
            (vectorset_id, config.vectorset_index_config.vector_dimension)
            async for vectorset_id, config in datamanagers.vectorsets.iter(txn, kbid=kbid)
        ]

    for vectorset_id, dimension in vectorsets:
        await vector_views.ensure_view(
            driver, driver.kb_database(kbid), vectorset_id, dimension, force=True
        )
