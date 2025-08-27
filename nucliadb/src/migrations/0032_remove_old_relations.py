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

"""Migration #32

Remove the old resource relations keys from maindb. It has been moved to user_relations which
is stored in object storage.

"""

import logging
from typing import Optional

from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None:
    start: Optional[str] = ""
    while True:
        if start is None:
            break
        start = await do_batch(context, start)


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    pass


async def do_batch(context: ExecutionContext, start: str) -> Optional[str]:
    logger.info(f"Running batch from {start}")
    async with context.kv_driver.rw_transaction() as txn:
        async with txn.connection.cursor() as cur:  # type: ignore
            # Retrieve a batch of fields
            await cur.execute(
                """
                SELECT key FROM resources
                WHERE key ~ '^/kbs/[^/]*/r/[^/]*/relations$'
                AND key > %s
                ORDER BY key
                LIMIT 500""",
                (start,),
            )
            records = await cur.fetchall()
            if len(records) == 0:
                return None

            keys = [r[0] for r in records]
            await cur.execute(
                "DELETE FROM resources WHERE key = ANY (%s)",
                (keys,),
            )
            await txn.commit()

            return keys[-1]
