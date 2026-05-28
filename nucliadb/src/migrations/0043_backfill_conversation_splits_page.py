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

"""Migration #43

Backfill the `page` field in SplitsMetadata entries for conversation fields.

Previously, SplitsMetadata tracked which message idents existed but did not
record which page each message belongs to. This migration iterates all
conversation fields and sets the correct page number on each split metadata
entry, enabling efficient page-targeted deletions.
"""

import logging
from typing import cast

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb.ingest.fields.conversation import (
    Conversation,
)
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos import resources_pb2

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
            async with context.kv_driver.rw_transaction() as txn:
                try:
                    await backfill_splits_page(txn, context.blob_storage, kbid, rid, field_id)
                    await txn.commit()
                except Exception:
                    logger.exception(
                        "Failed to backfill splits page metadata",
                        extra={"kbid": kbid, "rid": rid, "field_id": field_id},
                    )


async def backfill_splits_page(txn: Transaction, storage, kbid: str, rid: str, field_id: str) -> None:
    kb_orm = KnowledgeBoxORM(txn, storage, kbid)
    resource_obj = await kb_orm.get(rid)
    if resource_obj is None:
        return

    field_obj: Conversation = await resource_obj.get_field(
        field_id, resources_pb2.FieldType.CONVERSATION, load=False
    )
    splits_metadata = await field_obj.get_splits_metadata()
    conv_metadata = await field_obj.get_metadata()
    for page_number in range(1, conv_metadata.pages + 1):
        page = await field_obj.get_value(page=page_number)
        if page is None:
            continue
        for message in page.messages:
            splits_metadata.metadata.get_or_create(message.ident).page = page_number
    await field_obj.set_splits_metadata(splits_metadata)
