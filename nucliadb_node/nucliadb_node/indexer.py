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

import asyncio
import time
from dataclasses import dataclass
from functools import total_ordering
from typing import Optional

from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from nats.aio.client import Msg
from nucliadb_protos.noderesources_pb2 import ResourceID
from nucliadb_protos.nodewriter_pb2 import (
    IndexMessage,
    IndexMessageSource,
    OpStatus,
    TypeMessage,
)

from nucliadb_node import SERVICE_NAME, logger, signals
from nucliadb_node.signals import SuccessfulIndexingPayload
from nucliadb_node.writer import Writer
from nucliadb_telemetry import errors
from nucliadb_utils.nats import DemuxProcessor, NatsDemultiplexer
from nucliadb_utils.utilities import get_storage

# Messages coming from processor take loner to index, so we want to prioritize
# small messages coming from the writer (user creations/updates/deletes).
#
# Priority order: lower values first
PRIORITIES = {
    IndexMessageSource.WRITER: 0,
    IndexMessageSource.PROCESSOR: 1,
}


@total_ordering
@dataclass
class IndexerWorkUnit:
    seqid: int
    index_message: IndexMessage

    def priority(self) -> int:
        return PRIORITIES[self.index_message.source]

    def __eq__(self, other):
        return self.priority().__eq__(other.priority())

    def __lt__(self, other):
        return self.priority().__lt__(other.priority())

    def __hash__(self):
        return self.seqid.__hash__()


class ConcurrentShardIndexer(DemuxProcessor):
    def __init__(self, writer: Writer):
        self.writer = writer
        self.nats_demux = NatsDemultiplexer(
            processor=self,
            queue_klass=asyncio.PriorityQueue,
        )

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)

    async def finalize(self):
        pass

    def index_message_nowait(self, msg: Msg):
        self.nats_demux.handle_message_nowait(msg)

    def splitter(self, msg: Msg) -> tuple[str, IndexerWorkUnit]:
        seqid = int(msg.reply.split(".")[5])

        pb = IndexMessage()
        pb.ParseFromString(msg.data)
        split = pb.shard

        work = IndexerWorkUnit(seqid=seqid, index_message=pb)
        return (split, work)

    async def process(self, work: IndexerWorkUnit) -> bool:
        return await self.index_message(work)

    async def index_message(self, work: IndexerWorkUnit) -> bool:
        start = time.time()
        pb = work.index_message

        if pb.typemessage == TypeMessage.CREATION:
            await self.set_resource(pb)
        elif pb.typemessage == TypeMessage.DELETION:
            await self.delete_resource(pb)
        else:
            logger.warning(
                f"Unknown type message {pb.typemessage}",
                extra={
                    "seqid": work.seqid,
                    "shard": work.index_message.shard,
                },
            )

        await signals.successful_indexing.dispatch(
            SuccessfulIndexingPayload(seqid=work.seqid, index_message=pb)
        )
        logger.info(
            "Message indexing finished",
            extra={
                "seqid": work.seqid,
                "shard": pb.shard,
                "storage_key": pb.storage_key,
                "time": time.time() - start,
            },
        )
        return True

    async def set_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        brain = await self.storage.get_indexing(pb)
        shard_id = pb.shard
        rid = brain.resource.uuid
        brain.shard_id = brain.resource.shard_id = shard_id

        logger.info(f"Adding {rid} at {shard_id} otx:{pb.txid}")
        try:
            status = await self.writer.set_resource(brain)

        except AioRpcError as grpc_error:
            if grpc_error.code() == StatusCode.NOT_FOUND:
                logger.error(f"Shard does not exist {pb.shard}")
            else:
                event_id = errors.capture_exception(grpc_error)
                logger.error(
                    "An error ocurred on indexer worker while setting a resource. "
                    f"Check sentry for more details. Event id: {event_id}"
                )
                if brain.HasField("metadata"):
                    # Hard fail if we have the correct data
                    raise grpc_error
            return None

        else:
            logger.info(f"...done (Added {rid} at {shard_id} otx:{pb.txid})")
            return status

    async def delete_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        shard_id = pb.shard
        rid = pb.resource
        resource = ResourceID(uuid=rid, shard_id=shard_id)

        logger.info(f"Deleting {rid} in {shard_id} otx:{pb.txid}")
        try:
            status = await self.writer.delete_resource(resource)

        except AioRpcError as grpc_error:
            if grpc_error.code() == StatusCode.NOT_FOUND:
                logger.error(f"Shard does not exist {pb.shard}")
            else:
                event_id = errors.capture_exception(grpc_error)
                logger.error(
                    "An error ocurred on indexer worker while deleting a resource. "
                    f"Check sentry for more details. Event id: {event_id}"
                )
            return None

        else:
            logger.info(f"...done (Deleted {rid} in {shard_id} otx:{pb.txid})")
            return status
