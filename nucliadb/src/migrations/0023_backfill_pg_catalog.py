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

"""Migration #23

Backfill the data into the PG catalog

"""

import logging
from typing import cast

from nucliadb.common import datamanagers
from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.ingest.orm.processor.pgcatalog import pgcatalog_update
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if not isinstance(context.kv_driver, PGDriver):
        return

    BATCH_SIZE = 100
    async with context.kv_driver.transaction() as txn:
        txn = cast(PGTransaction, txn)
        continue_sql = ""
        while True:
            async with txn.connection.cursor() as cur:
                # Get list of resources except those already in the catalog
                await cur.execute(
                    f"""
                    SELECT SPLIT_PART(key, '/', 5)::UUID FROM resources
                    LEFT JOIN catalog ON kbid = %s AND SPLIT_PART(key, '/', 5)::UUID = rid
                    WHERE key SIMILAR TO %s
                    AND rid IS NULL
                    {continue_sql}
                    ORDER BY key
                    LIMIT %s
                    """,
                    (kbid, f"/kbs/{kbid}/r/[a-f0-9]*", BATCH_SIZE),
                )
                resources_to_index = [r[0] for r in await cur.fetchall()]
                if len(resources_to_index) == 0:
                    return

                # Index each resource
                for rid in resources_to_index:
                    rid = str(rid).replace("-", "")
                    resource = await datamanagers.resources.get_resource(txn, kbid=kbid, rid=rid)
                    if resource is None:
                        logger.warning(f"Could not load resource {rid} for kbid {kbid}")
                        continue

                    await resource.compute_global_tags(resource.indexer)
                    await pgcatalog_update(txn, kbid, resource)

                await txn.commit()
                continue_sql = f"AND key > '/kbs/{kbid}/r/{rid}'"
