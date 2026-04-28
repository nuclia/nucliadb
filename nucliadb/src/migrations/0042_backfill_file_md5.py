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

from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.resource import Resource
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos.resources_pb2 import FieldType

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if not isinstance(context.kv_driver, PGDriver):
        return

    BATCH_SIZE = 100
    inserted = 0

    async with context.kv_driver.rw_transaction() as txn:
        txn = cast(PGTransaction, txn)
        continue_sql = ""
        while True:
            async with txn.connection.cursor() as cur:
                await cur.execute(
                    f"""
                    SELECT SPLIT_PART(key, '/', 5)::UUID FROM resources
                    WHERE key SIMILAR TO %s
                    {continue_sql}
                    ORDER BY key
                    LIMIT %s
                    """,
                    (f"/kbs/{kbid}/r/[a-f0-9]*", BATCH_SIZE),
                )
                resource_uuids = [r[0] for r in await cur.fetchall()]
                if len(resource_uuids) == 0:
                    break

                for rid_uuid in resource_uuids:
                    rid = str(rid_uuid).replace("-", "")
                    resource = await Resource.get(txn, kbid=kbid, rid=rid)
                    if resource is None:
                        continue

                    fields_ids = await resource.get_fields_ids()
                    for field_type, field_id in fields_ids:
                        if field_type != FieldType.FILE:
                            continue
                        field = await resource.get_field(field_id, field_type)
                        if not isinstance(field, File):
                            continue
                        value = await field.get_value()
                        if value is None or not value.file.md5:
                            continue

                        async with txn.connection.cursor() as insert_cur:
                            await insert_cur.execute(
                                """
                                INSERT INTO file_md5 (kbid, md5, rid, field_id)
                                VALUES (%(kbid)s, %(md5)s, %(rid)s, %(field_id)s)
                                ON CONFLICT (kbid, md5, rid, field_id) DO NOTHING
                                """,
                                {
                                    "kbid": kbid,
                                    "md5": value.file.md5,
                                    "rid": rid,
                                    "field_id": field_id,
                                },
                            )
                            inserted += 1

                await txn.commit()
                continue_sql = f"AND key > '/kbs/{kbid}/r/{rid}'"

    logger.info(
        "Backfilled file_md5 entries",
        extra={"kbid": kbid, "inserted": inserted},
    )
