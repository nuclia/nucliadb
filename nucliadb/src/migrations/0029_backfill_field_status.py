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

"""Migration #29

Backfill field status (from error)
"""

import logging
from typing import Optional

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
    logger.info(f"Running batch from {start}")
    async with context.kv_driver.transaction(read_only=False) as txn:
        async with txn.connection.cursor() as cur:  # type: ignore
            # Retrieve a batch of fields
            await cur.execute(
                """
                SELECT key FROM resources
                WHERE key ~ '^/kbs/[^/]*/r/[^/]*/f/[^/]*/[^/]*$'
                AND key > %s
                ORDER BY key
                LIMIT 500""",
                (start,),
            )
            records = await cur.fetchall()
            if len(records) == 0:
                return None

            field_keys = [r[0] for r in records]

            # Retrieve resources basic (to check status)
            resource_keys = set(["/".join(f.split("/")[:5]) for f in field_keys])
            await cur.execute(
                """
                SELECT key, value FROM resources
                WHERE key = ANY (%s)
                ORDER BY key
                """,
                (list(resource_keys),),
            )
            records = await cur.fetchall()
            resources_basic = {}
            for k, v in records:
                row_basic = resources_pb2.Basic()
                row_basic.ParseFromString(v)
                resources_basic[k] = row_basic

            # Retrieve field errors
            await cur.execute(
                """
                SELECT key, value FROM resources
                WHERE key ~ '^/kbs/[^/]*/r/[^/]*/f/[^/]*/[^/]*/error$'
                AND key > %s AND key <= %s
                ORDER BY key
                """,
                (start, field_keys[-1] + "/error"),
            )
            records = await cur.fetchall()
            errors = {}
            for k, v in records:
                row_error = writer_pb2.Error()
                row_error.ParseFromString(v)
                errors[k] = row_error

            # Retrieve existing status keys
            await cur.execute(
                """
                SELECT key FROM resources
                WHERE key ~ '^/kbs/[^/]*/r/[^/]*/f/[^/]*/[^/]*/status$'
                AND key > %s AND key <= %s
                ORDER BY key
                """,
                (start, field_keys[-1] + "/status"),
            )
            records = await cur.fetchall()
            has_status = [r[0] for r in records]

            set_batch = []
            for field_key in field_keys:
                if field_key + "/status" in has_status:
                    # Already has status, skip
                    continue

                resource_key = "/".join(field_key.split("/")[:5])
                basic = resources_basic.get(resource_key, None)
                if basic is None:
                    logger.warn(f"{field_key} resource has no basic, skipped")
                    continue

                status = writer_pb2.FieldStatus()
                status.status = writer_pb2.FieldStatus.Status.PROCESSED
                error = errors.get(field_key + "/error", None)
                # We only copy errors if they come from data augmentation or if the resource is in error
                # This way we ensure we do not set an error for resources that were previously not in error
                # There is no way to do this 100% accurate since the /error key is only cleared on field deletion
                if error:
                    if (
                        error.code == writer_pb2.Error.ErrorCode.DATAAUGMENTATION
                        or basic.metadata.status == resources_pb2.Metadata.Status.ERROR
                    ):
                        field_error = writer_pb2.FieldError(
                            source_error=error,
                        )
                        status.errors.append(field_error)
                        status.status = writer_pb2.FieldStatus.Status.ERROR
                set_batch.append((field_key + "/status", status.SerializeToString()))

            # Write everything to the database in batch
            async with cur.copy("COPY resources (key, value) FROM STDIN") as copy:
                for row in set_batch:
                    await copy.write_row(row)
            await txn.commit()

            return field_keys[-1]
