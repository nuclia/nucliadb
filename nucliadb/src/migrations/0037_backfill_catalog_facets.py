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

"""Migration #37

Backfill catalog facets

"""

import logging
from typing import cast

from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None:
    driver = cast(PGDriver, context.kv_driver)

    BATCH_SIZE = 1_000
    async with driver.rw_transaction() as txn:
        txn = cast(PGTransaction, txn)
        start_kbid = "00000000000000000000000000000000"
        start_rid = "00000000000000000000000000000000"
        while True:
            async with txn.connection.cursor() as cur:
                logger.info(f"Filling {BATCH_SIZE} catalog facets from {start_kbid}, {start_rid}")
                # Get a batch of facets from the catalog table
                await cur.execute(
                    """
                        WITH i AS (
                            INSERT INTO catalog_facets (kbid, rid, facet)
                            SELECT kbid, rid, unnest(extract_facets(labels)) FROM (
                                SELECT * FROM catalog
                                WHERE (kbid = %(kbid)s AND rid > %(rid)s) OR kbid > %(kbid)s
                                ORDER BY kbid, rid
                                LIMIT %(batch)s
                            ) rs
                            RETURNING kbid, rid
                        )
                        SELECT kbid, rid FROM i ORDER BY kbid DESC, rid DESC LIMIT 1;
                    """,
                    {"kbid": start_kbid, "rid": start_rid, "batch": BATCH_SIZE},
                )

                # Set the key for next iteration
                results = await cur.fetchone()  # type: ignore
                if results is None:
                    break
                (start_kbid, start_rid) = results

                await txn.commit()


async def migrate_kb(context: ExecutionContext, kbid: str) -> None: ...
