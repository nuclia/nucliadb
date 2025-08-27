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

"""Migration #36

Backfill catalog slug field

"""

import logging
from typing import cast

from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None:
    driver = cast(PGDriver, context.kv_driver)

    BATCH_SIZE = 10_000
    async with driver.rw_transaction() as txn:
        txn = cast(PGTransaction, txn)
        start_key = ""
        while True:
            async with txn.connection.cursor() as cur:
                logger.info(f"Filling {BATCH_SIZE} catalog slugs from {start_key}")
                # Get a batch of slugs from the resource table
                await cur.execute(
                    """
                        CREATE TEMPORARY TABLE tmp_0036_backfill_catalog ON COMMIT DROP AS
                        SELECT
                            key,
                            SPLIT_PART(key, '/', 3)::UUID AS kbid,
                            SPLIT_PART(key, '/', 5) AS slug,
                            ENCODE(value, 'escape')::UUID AS rid
                        FROM resources
                        WHERE key ~ '^/kbs/[^/]+/s/.*'
                        AND key > %s
                        ORDER BY key
                        LIMIT %s
                    """,
                    (start_key, BATCH_SIZE),
                )

                # Set the key for next iteration
                await cur.execute("SELECT MAX(key) FROM tmp_0036_backfill_catalog")
                start_key = (await cur.fetchone())[0]  # type: ignore
                if start_key is None:
                    break

                # Update the catalog with the slugs
                await cur.execute(
                    """
                        UPDATE catalog c SET slug = tmp.slug
                        FROM tmp_0036_backfill_catalog tmp
                        WHERE c.kbid = tmp.kbid AND c.rid = tmp.rid
                    """
                )
                await txn.commit()


async def migrate_kb(context: ExecutionContext, kbid: str) -> None: ...
