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

"""Migration #51

Remove all the leftover keys from the old key/value implementation of resources,
fields, conversations and kb/resource slugs. This data now lives exclusively in the
`kbs`, `kb_resources`, `kb_fields` and `kb_conversations` PostgreSQL tables (see
migration 0016 and proposals/records/007-orm-tables-over-kv.md).

The removed key patterns are:
  - /kbs/{kbid}/r/{uuid}                       - resource Basic blob and everything nested
                                                  under it (shard, origin, extra, security,
                                                  allfields, fields, conversation pages,
                                                  user metadata, field status/error...)
  - /kbs/{kbid}/s/{slug}                       - resource slug -> uuid mapping
  - /kbslugs/{slug}                            - KB slug -> kbid mapping
  - /kbs/{kbid}/materialized/resources/count   - materialized resource count
  - /kbs/{kbid}/config                         - legacy KB uuid/config marker
"""

import logging

from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)

BATCH_SIZE = 500

KEY_PATTERNS = (
    r"^/kbs/[^/]*/r/.*$",
    r"^/kbs/[^/]*/s/.*$",
    r"^/kbslugs/.*$",
    r"^/kbs/[^/]*/materialized/resources/count$",
    r"^/kbs/[^/]*/config$",
)


async def migrate(context: ExecutionContext) -> None:
    for pattern in KEY_PATTERNS:
        start: str | None = ""
        while True:
            if start is None:
                break
            start = await do_batch(context, pattern, start)


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    pass


async def do_batch(context: ExecutionContext, pattern: str, start: str) -> str | None:
    logger.info(f"Running batch for pattern {pattern} from {start}")
    async with context.kv_driver.rw_transaction() as txn:
        async with txn.connection.cursor() as cur:  # type: ignore
            await cur.execute(
                """
                SELECT key FROM resources
                WHERE key ~ %s
                AND key > %s
                ORDER BY key
                LIMIT %s""",
                (pattern, start, BATCH_SIZE),
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
