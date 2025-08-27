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

"""Migration #38

Backfill the catalog with labels from fields metadata

"""

import logging
from typing import cast

from nucliadb.common import datamanagers
from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.ingest.orm.index_message import get_resource_index_message
from nucliadb.ingest.orm.processor.pgcatalog import pgcatalog_update
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos import resources_pb2

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if not isinstance(context.kv_driver, PGDriver):
        return

    BATCH_SIZE = 100
    async with context.kv_driver.rw_transaction() as txn:
        txn = cast(PGTransaction, txn)
        start = ""
        while True:
            async with txn.connection.cursor() as cur:
                # Get list of resources except those already in the catalog
                await cur.execute(
                    """
                    SELECT key, value FROM resources
                    WHERE key ~ ('^/kbs/' || %s || '/r/[^/]*$')
                    AND key > %s
                    ORDER BY key
                    LIMIT %s""",
                    (kbid, start, BATCH_SIZE),
                )

                to_index = []
                rows = await cur.fetchall()
                if len(rows) == 0:
                    return
                for key, basic_pb in rows:
                    start = key

                    # Only reindex resources with labels in field computed metadata
                    basic = resources_pb2.Basic()
                    basic.ParseFromString(basic_pb)
                    if basic.computedmetadata.field_classifications:
                        to_index.append(key)

                logger.info(f"Reindexing {len(to_index)} catalog entries from {start}")
                # Index each resource
                for key in to_index:
                    rid = key.split("/")[4]
                    resource = await datamanagers.resources.get_resource(txn, kbid=kbid, rid=rid)
                    if resource is None:
                        logger.warning(f"Could not load resource {rid} for kbid {kbid}")
                        continue

                    index_message = await get_resource_index_message(resource, reindex=False)
                    await pgcatalog_update(txn, kbid, resource, index_message)

                if to_index:
                    await txn.commit()
