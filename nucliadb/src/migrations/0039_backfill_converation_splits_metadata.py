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

"""Migration #39

Backfill splits metadata on conversation fields

"""

import logging
from typing import cast

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb.ingest.fields.conversation import (
    CONVERSATION_SPLITS_METADATA,
    Conversation,
)
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import SplitsMetadata
from nucliadb_utils.storages.storage import Storage

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    BATCH_SIZE = 100
    start = ""
    while True:
        to_fix: list[tuple[str, str]] = []
        async with context.kv_driver.rw_transaction() as txn:
            txn = cast(PGTransaction, txn)
            async with txn.connection.cursor() as cur:
                # Retrieve a bunch of conversation fields
                await cur.execute(
                    """
                    SELECT key FROM resources
                    WHERE key ~ ('^/kbs/' || %s || '/r/[^/]*/f/c/[^/]*$')
                    AND key > %s
                    ORDER BY key
                    LIMIT %s""",
                    (kbid, start, BATCH_SIZE),
                )
                rows = await cur.fetchall()
                if len(rows) == 0:
                    return
                for row in rows:
                    key = row[0]
                    start = key
                    rid = key.split("/")[4]
                    field_id = key.split("/")[7]
                    to_fix.append((rid, field_id))

        for rid, field_id in to_fix:
            async with context.kv_driver.rw_transaction() as txn2:
                splits_metadata = await build_splits_metadata(
                    txn2, context.blob_storage, kbid, rid, field_id
                )
                splits_metadata_key = CONVERSATION_SPLITS_METADATA.format(
                    kbid=kbid, uuid=rid, type="c", field=field_id
                )
                await txn2.set(splits_metadata_key, splits_metadata.SerializeToString())
                await txn2.commit()


async def build_splits_metadata(
    txn: Transaction, storage: Storage, kbid: str, rid: str, field_id: str
) -> SplitsMetadata:
    splits_metadata = SplitsMetadata()
    kb_orm = KnowledgeBoxORM(txn, storage, kbid)
    resource_obj = await kb_orm.get(rid)
    if resource_obj is None:
        return splits_metadata
    field_obj: Conversation = await resource_obj.get_field(
        field_id, resources_pb2.FieldType.CONVERSATION, load=False
    )
    conv_metadata = await field_obj.get_metadata()
    for i in range(1, conv_metadata.pages + 1):
        page = await field_obj.get_value(page=i)
        if page is None:
            continue
        for message in page.messages:
            splits_metadata.metadata.get_or_create(message.ident)
    return splits_metadata
