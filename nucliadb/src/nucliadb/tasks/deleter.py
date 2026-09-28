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
from datetime import datetime
from typing import Iterable
from uuid import UUID, uuid4

from pydantic import Base64Bytes, BaseModel

from nucliadb.common import datamanagers
from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.driver import Driver
from nucliadb.tasks import create_consumer
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.logger import logger
from nucliadb.tasks.producer import create_producer
from nucliadb.tasks.utils import NatsConsumer, NatsStream
from nucliadb.writer.api.v1 import transaction
from nucliadb.writer.utilities import get_processing
from nucliadb_protos.writer_pb2 import Audit, BrokerMessage
from nucliadb_utils.utilities import get_partitioning

BATCH_SIZE = 20


class DeleterNatsConfig:
    stream = NatsStream(name="ndb-tasks", subjects=["delete.>"])
    consumer = NatsConsumer(subject="delete.resources", group="ndb-batch-deleter")


# maindb key for deletion metadata
DELETION_METADATA = "kbs/{kbid}/task/batch_delete/{deletion_id}"


class DeleteBatch(BaseModel):
    kbid: str
    deletion_id: UUID


class BatchDeletionMetadata(BaseModel):
    deleted: int
    pending: set[str]
    total: int
    requested_at: datetime
    serialized_audit_pb: Base64Bytes


def deleter_consumer() -> NatsTaskConsumer[DeleteBatch]:
    consumer: NatsTaskConsumer[DeleteBatch] = create_consumer(
        name="batch_delete_creator",
        stream=DeleterNatsConfig.stream,
        consumer=DeleterNatsConfig.consumer,
        callback=batch_deleter_task,
        msg_type=DeleteBatch,
        max_concurrent_messages=1,
        max_retries=5,
    )
    return consumer


async def schedule_batch_delete(
    context: ApplicationContext, kbid: str, resources: set[str], audit: Audit
) -> UUID:
    # A batch delete job maintains it's status in maindb to avoid repeating
    # deletes on retries. As we already need to maintain this data, we use a
    # small NATS message just to notify a deletion and don't include the list of
    # resources there.

    deletion_id = uuid4()
    metadata = BatchDeletionMetadata(
        deleted=0,
        pending=resources,
        total=len(resources),
        requested_at=datetime.now(),
        serialized_audit_pb=base64.b64encode(audit.SerializeToString()),
    )
    await set_deletion_metadata(context.kv_driver, kbid, deletion_id, metadata)

    producer = create_producer(
        name="batch_delete_creator",
        stream=DeleterNatsConfig.stream,
        producer_subject=DeleterNatsConfig.consumer.subject,
        msg_type=DeleteBatch,
    )
    msg = DeleteBatch(kbid=kbid, deletion_id=deletion_id)
    try:
        await producer.send(msg)
    except Exception:
        await delete_deletion_metadata(context.kv_driver, kbid, deletion_id)
        raise

    return deletion_id


async def batch_deleter_task(context: ApplicationContext, msg: DeleteBatch):
    partitioning = get_partitioning()
    processing = get_processing()

    kbid = msg.kbid
    deletion_id = msg.deletion_id
    metadata = await get_deletion_metadata(context.kv_driver, kbid, deletion_id)
    if metadata is None:
        logger.warning(
            "Trying to run a batch deletion but no metadata found in maindb, "
            "is this a retry of a successful not-acked job?",
            extra={
                "kbid": kbid,
                "deletion_id": deletion_id,
            },
        )
        return

    logger.info("Running batch deletion job", extra={"kbid": kbid, "deletion_id": deletion_id})

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

    # TODO: handle retries on failed deletions

    # cleanup maindb
    await delete_deletion_metadata(context.kv_driver, kbid, deletion_id)

    logger.info(
        "Batch deletion job completed",
        extra={"kbid": kbid, "deletion_id": deletion_id, "deleted": metadata.deleted},
    )


async def get_deletion_metadata(
    driver: Driver, kbid: str, deletion_id: UUID
) -> BatchDeletionMetadata | None:
    async with driver.ro_transaction() as txn:
        raw = await txn.get(DELETION_METADATA.format(kbid=kbid, deletion_id=deletion_id))
        if raw is None:
            return None
        return BatchDeletionMetadata.model_validate_json(raw)


async def set_deletion_metadata(
    driver: Driver, kbid: str, deletion_id: UUID, metadata: BatchDeletionMetadata
):
    async with driver.rw_transaction() as txn:
        raw = metadata.model_dump_json().encode()
        await txn.set(DELETION_METADATA.format(kbid=kbid, deletion_id=deletion_id), raw)
        await txn.commit()


async def delete_deletion_metadata(driver: Driver, kbid: str, deletion_id: UUID):
    async with driver.rw_transaction() as txn:
        await txn.delete(DELETION_METADATA.format(kbid=kbid, deletion_id=deletion_id))
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
