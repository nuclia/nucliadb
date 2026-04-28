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

"""Migration #42

Backfill the file_md5 table with MD5 hashes from existing file fields.
"""

import logging
from typing import cast

from nucliadb.common import datamanagers, file_md5
from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos.resources_pb2 import FieldType, FieldFile

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if not isinstance(context.kv_driver, PGDriver):
        return

    BATCH_SIZE = 100
    inserted = 0
    pages = 0

    async with context.kv_driver.rw_transaction() as txn:
        txn = cast(PGTransaction, txn)
        last_key = ""
        while True:
            async with txn.connection.cursor() as cur:
                await cur.execute(
                    """
                    SELECT key FROM resources
                    WHERE key LIKE %s
                    AND key > %s
                    ORDER BY key
                    LIMIT %s
                    """,
                    (f"/kbs/{kbid}/r/%/f/f/%", last_key, BATCH_SIZE),
                )
                rows = await cur.fetchall()
                if len(rows) == 0:
                    break

                for (key,) in rows:
                    last_key = key
                    # key format: /kbs/{kbid}/r/{rid}/f/f/{field_id}
                    parts = key.split("/")
                    # ['', 'kbs', kbid, 'r', rid, 'f', 'f', field_id]
                    rid = parts[4]
                    field_id = parts[7]
                    # Get the field value to extract the MD5 hash
                    payload = await datamanagers.fields.get_raw(
                        txn,
                        kbid=kbid,
                        rid=rid,
                        field_type=FieldType.FILE,
                        field_id=field_id,
                    )
                    if payload is None or len(payload) == 0:
                        # No payload found for this field, skip it
                        continue
                    value: FieldFile  = FieldFile.ParseFromString(payload)
                    if not value.file.md5:
                        # No md5 hash set to backfill for this field
                        continue
                    await file_md5.set(
                        txn, kbid=kbid, md5=value.file.md5, rid=rid, field_id=field_id
                    )
                    inserted += 1

                await txn.commit()
                pages += 1
                if pages % 10 == 0:
                    logger.info(
                        "Backfill file_md5 in progress",
                        extra={"kbid": kbid, "pages": pages, "inserted": inserted},
                    )

    logger.info(
        "Backfilled file_md5 entries",
        extra={"kbid": kbid, "inserted": inserted},
    )
