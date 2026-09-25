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

import base64
import itertools
from typing import Iterable
from uuid import UUID, uuid4

from pydantic import Base64Bytes, BaseModel

from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.driver import Driver
from nucliadb.writer.api.v1 import transaction
from nucliadb.writer.utilities import get_processing
from nucliadb_protos.writer_pb2 import Audit, BrokerMessage
from nucliadb_utils.utilities import get_partitioning


class ResourceBatch(BaseModel):
    kbid: str
    deletion_id: UUID


class BatchDeletionMetadata(BaseModel):
    deleted: int
    pending: set[str]
    total: int
    serialized_audit_pb: Base64Bytes


BATCH_SIZE = 2

METADATA = "kbs/{kbid}/batch_delete/{deletion_id}"


async def schedule_batch_delete(
    context: ApplicationContext, kbid: str, resources: set[str], audit: Audit
) -> UUID:
    # TODO: nats stuff

    deletion_id = uuid4()
    metadata = BatchDeletionMetadata(
        deleted=0,
        pending=resources,
        total=len(resources),
        serialized_audit_pb=base64.b64encode(audit.SerializeToString()),
    )
    await set_deletion_metadata(context.kv_driver, kbid, deletion_id, metadata)

    return deletion_id


async def batch_deleter_task(context: ApplicationContext, msg: ResourceBatch):
    partitioning = get_partitioning()
    processing = get_processing()

    kbid = msg.kbid
    deletion_id = msg.deletion_id
    metadata = await get_deletion_metadata(context.kv_driver, kbid, deletion_id)
    if metadata is None:
        # TODO: ?
        return

    audit = Audit()
    audit.ParseFromString(metadata.serialized_audit_pb)
    pending = metadata.pending.copy()
    for batch in batched(pending, BATCH_SIZE):
        # TODO: backoff
        for rid in batch:
            if not (await datamanagers.atomic.resources.exists(kbid=kbid, rid=rid)):
                # already deleted, skipping
                metadata.pending.remove(rid)
                continue

            writer = BrokerMessage()
            writer.kbid = kbid
            writer.uuid = rid
            writer.type = BrokerMessage.MessageType.DELETE
            writer.audit.CopyFrom(audit)

            partition = partitioning.generate_partition(kbid, rid)
            # TODO: handle exceptions on commit (retry later, backoff...)
            await transaction.commit(writer, partition)

            await processing.delete_from_processing(kbid=kbid, resource_id=rid)

            metadata.deleted += 1
            metadata.pending.remove(rid)

        # Update task progress
        await set_deletion_metadata(context.kv_driver, kbid, deletion_id, metadata)


async def get_deletion_metadata(
    driver: Driver, kbid: str, deletion_id: UUID
) -> BatchDeletionMetadata | None:
    async with driver.ro_transaction() as txn:
        raw = await txn.get(METADATA.format(kbid=kbid, deletion_id=deletion_id))
        if raw is None:
            return None
        return BatchDeletionMetadata.model_validate_json(raw)


async def set_deletion_metadata(
    driver: Driver, kbid: str, deletion_id: UUID, metadata: BatchDeletionMetadata
):
    async with driver.rw_transaction() as txn:
        raw = metadata.model_dump_json().encode()
        await txn.set(METADATA.format(kbid=kbid, deletion_id=deletion_id), raw)
        await txn.commit()


# Adapted from itertools docs.
# TODO: Replace with itertools.batched once we are at Python>=3.12
def batched(iterable: Iterable, n: int):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        yield batch
