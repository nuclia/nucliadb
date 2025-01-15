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

"""Migration #28

Backfill field status (from error)
"""

import logging
from typing import Optional

from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos import resources_pb2, writer_pb2

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None:
    start: Optional[str] = ""
    while True:
        if start is None:
            break
        start = await do_batch(context, start)


async def migrate_kb(context: ExecutionContext, kbid: str) -> None: ...


async def do_batch(context: ExecutionContext, start: str) -> Optional[str]:
    async with context.kv_driver.transaction(read_only=False) as txn:
        async with txn.connection.cursor() as cur:  # type: ignore
            await cur.execute(
                """
                SELECT key, value FROM resources
                WHERE key ~ '^/kbs/[^/]*/r/[^/]*/f/[^/]*/[^/]*/error$'
                AND key > %s
                ORDER BY key
                LIMIT 100""",
                (start,),
            )
            records = await cur.fetchall()
            if len(records) == 0:
                return None

            print(f"Processing {len(records)} field errors from {start}")
            for key, value in records:
                start = key

                parts = key.split("/")
                parts[-1] = "status"
                status_key = "/".join(parts)

                existing_status = await txn.get(status_key)
                if existing_status is not None:
                    continue

                error = writer_pb2.Error()
                error.ParseFromString(value)

                # We do not copy non-DA errors if the resource is not in error.
                # If we did, we could set some resources that previously were fine to an error status
                # which is not desired.
                # There is no way to do this 100% accurate since the /error key is only cleared on field deletion
                if error.code != writer_pb2.Error.ErrorCode.DATAAUGMENTATION:
                    kbid = parts[2]
                    rid = parts[4]
                    basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
                    if basic is None or basic.metadata.status != resources_pb2.Metadata.Status.ERROR:
                        continue

                # A resource here is in error state, set the field status with the error
                status = writer_pb2.FieldStatus(
                    status=writer_pb2.FieldStatus.Status.ERROR,
                )
                field_error = writer_pb2.FieldError(
                    source_error=error,
                )
                field_error.created.GetCurrentTime()
                status.errors.append(field_error)
                await txn.set(status_key, status.SerializeToString())

            await txn.commit()
            return start
