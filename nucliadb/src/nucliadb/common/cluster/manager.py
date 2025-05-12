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
import asyncio
import logging
import uuid
from typing import Any, Awaitable, Callable, Optional

from nidx_protos import noderesources_pb2, nodewriter_pb2
from nidx_protos.nodewriter_pb2 import (
    IndexMessage,
    IndexMessageSource,
    NewShardRequest,
    NewVectorSetRequest,
    TypeMessage,
)

from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import (
    NodeError,
    ShardsNotFound,
)
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.nidx import get_nidx, get_nidx_api_client
from nucliadb.common.vector_index_config import nucliadb_index_config_to_nidx
from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_telemetry import errors
from nucliadb_utils.utilities import get_storage

from .settings import settings

logger = logging.getLogger(__name__)


class KBShardManager:
    # TODO: move to data manager
    async def get_shards_by_kbid_inner(self, kbid: str) -> writer_pb2.Shards:
        async with datamanagers.with_ro_transaction() as txn:
            result = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
            if result is None:
                # could be None because /shards doesn't exist, or beacause the
                # whole KB does not exist. In any case, this should not happen
                raise ShardsNotFound(kbid)
            return result

    # TODO: move to data manager
    async def get_shards_by_kbid(self, kbid: str) -> list[writer_pb2.ShardObject]:
        shards = await self.get_shards_by_kbid_inner(kbid)
        return [x for x in shards.shards]

    async def apply_for_all_shards(
        self,
        kbid: str,
        aw: Callable[[str], Awaitable[Any]],
        timeout: float,
    ) -> list[Any]:
        shards = await self.get_shards_by_kbid(kbid)
        ops = []

        for shard_obj in shards:
            ops.append(aw(shard_obj.nidx_shard_id))

        try:
            results = await asyncio.wait_for(
                asyncio.gather(*ops, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            errors.capture_exception(exc)
            raise NodeError("Node unavailable for operation") from exc

        for result in results:
            if isinstance(result, Exception):
                errors.capture_exception(result)
                raise NodeError(
                    f"Error while applying {aw.__name__} for all shards. Other similar errors may have been shadowed.\n"
                    f"{type(result).__name__}: {result}"
                ) from result

        return results

    # TODO: move to data manager
    async def get_current_active_shard(
        self, txn: Transaction, kbid: str
    ) -> Optional[writer_pb2.ShardObject]:
        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=False)
        if kb_shards is None:
            return None

        # B/c with Shards.actual
        # Just ignore the new attribute for now
        shard = kb_shards.shards[kb_shards.actual]
        return shard

    # TODO: logic about creation and read-only shards should be decoupled
    async def create_shard_by_kbid(
        self,
        txn: Transaction,
        kbid: str,
    ) -> writer_pb2.ShardObject:
        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=True)
        if kb_shards is None:
            msg = ("Attempting to create a shard for a KB when it has no stored shards in maindb",)
            logger.error(msg, extra={"kbid": kbid})
            raise ShardsNotFound(msg)

        vectorsets = {
            vectorset_id: nucliadb_index_config_to_nidx(vectorset_config.vectorset_index_config)
            async for vectorset_id, vectorset_config in datamanagers.vectorsets.iter(txn, kbid=kbid)
        }

        shard_uuid = uuid.uuid4().hex

        shard = writer_pb2.ShardObject(shard=shard_uuid, read_only=False)
        try:
            nidx_api = get_nidx_api_client()
            req = NewShardRequest(
                kbid=kbid,
                vectorsets_configs=vectorsets,
            )

            resp = await nidx_api.NewShard(req)  # type: ignore
            shard.nidx_shard_id = resp.id

        except Exception as exc:
            errors.capture_exception(exc)
            logger.exception(f"Unexpected error creating new shard for KB", extra={"kbid": kbid})
            await self.rollback_shard(shard)
            raise exc

        # set previous shard as read only, we only have one writable shard at a
        # time
        if len(kb_shards.shards) > 0:
            kb_shards.shards[-1].read_only = True

        # Append the created shard and make `actual` point to it.
        kb_shards.shards.append(shard)
        # B/c with Shards.actual - we only use last created shard
        kb_shards.actual = len(kb_shards.shards) - 1

        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=kb_shards)

        return shard

    async def rollback_shard(self, shard: writer_pb2.ShardObject):
        nidx_api = get_nidx_api_client()
        try:
            await nidx_api.DeleteShard(noderesources_pb2.ShardId(id=shard.nidx_shard_id))
        except Exception as rollback_error:
            errors.capture_exception(rollback_error)
            logger.error(
                f"New shard rollback error. Nidx Shard: {shard.nidx_shard_id}",
                exc_info=True,
            )

    async def delete_resource(
        self,
        shard: writer_pb2.ShardObject,
        uuid: str,
        txid: int,
        partition: str,
        kb: str,
    ) -> None:
        storage = await get_storage()
        nidx = get_nidx()

        await storage.delete_indexing(resource_uid=uuid, txid=txid, kb=kb, logical_shard=shard.shard)

        nidxpb: nodewriter_pb2.IndexMessage = nodewriter_pb2.IndexMessage()
        nidxpb.shard = shard.nidx_shard_id
        nidxpb.resource = uuid
        nidxpb.typemessage = nodewriter_pb2.TypeMessage.DELETION
        await nidx.index(nidxpb)

    async def add_resource(
        self,
        shard: writer_pb2.ShardObject,
        resource: noderesources_pb2.Resource,
        txid: int,
        partition: str,
        kb: str,
        reindex_id: Optional[str] = None,
        source: IndexMessageSource.ValueType = IndexMessageSource.PROCESSOR,
    ) -> None:
        """
        Stores the Resource object in the object storage and sends an IndexMessage to the indexing Nats stream.
        """
        if txid == -1 and reindex_id is None:
            # This means we are injecting a complete resource via ingest gRPC
            # outside of a transaction. We need to treat this as a reindex operation.
            reindex_id = uuid.uuid4().hex

        storage = await get_storage()
        nidx = get_nidx()
        indexpb = IndexMessage()

        if reindex_id is not None:
            storage_key = await storage.reindexing(
                resource, reindex_id, partition, kb=kb, logical_shard=shard.shard
            )
            indexpb.reindex_id = reindex_id
        else:
            storage_key = await storage.indexing(
                resource, txid, partition, kb=kb, logical_shard=shard.shard
            )
            indexpb.txid = txid

        indexpb.typemessage = TypeMessage.CREATION
        indexpb.storage_key = storage_key
        indexpb.kbid = kb
        if partition:
            indexpb.partition = partition
        indexpb.source = source
        indexpb.resource = resource.resource.uuid

        indexpb.shard = shard.nidx_shard_id
        await nidx.index(indexpb)

    def should_create_new_shard(self, num_paragraphs: int) -> bool:
        return num_paragraphs > settings.max_shard_paragraphs

    async def maybe_create_new_shard(
        self,
        kbid: str,
        num_paragraphs: int,
    ):
        if not self.should_create_new_shard(num_paragraphs):
            return

        logger.info({"message": "Adding shard", "kbid": kbid})

        async with datamanagers.with_transaction() as txn:
            await self.create_shard_by_kbid(txn, kbid)
            await txn.commit()

    async def create_vectorset(self, kbid: str, config: knowledgebox_pb2.VectorSetConfig):
        """Create a new vectorset in all KB shards."""

        async def _create_vectorset(shard_id: str):
            vectorset_id = config.vectorset_id
            index_config = nucliadb_index_config_to_nidx(config.vectorset_index_config)

            req = NewVectorSetRequest(
                id=noderesources_pb2.VectorSetID(
                    shard=noderesources_pb2.ShardId(id=shard_id), vectorset=vectorset_id
                ),
                config=index_config,
            )

            result = await get_nidx_api_client().AddVectorSet(req)
            if result.status != result.Status.OK:
                raise NodeError(
                    f"Unable to create vectorset {vectorset_id} in kb {kbid} shard {shard_id}"
                )

        await self.apply_for_all_shards(kbid, _create_vectorset, timeout=10)

    async def delete_vectorset(self, kbid: str, vectorset_id: str):
        """Delete a vectorset from all KB shards"""

        async def _delete_vectorset(shard_id: str):
            req = noderesources_pb2.VectorSetID()
            req.shard.id = shard_id
            req.vectorset = vectorset_id

            result = await get_nidx_api_client().RemoveVectorSet(req)
            if result.status != result.Status.OK:
                raise NodeError(
                    f"Unable to delete vectorset {vectorset_id} in kb {kbid} shard {shard_id}"
                )

        await self.apply_for_all_shards(kbid, _delete_vectorset, timeout=10)


class StandaloneKBShardManager(KBShardManager):
    max_ops_before_checks = 200

    def __init__(self: "StandaloneKBShardManager"):
        super().__init__()
        self._lock = asyncio.Lock()
        self._change_count: dict[tuple[str, str], int] = {}

    async def delete_resource(
        self,
        shard: writer_pb2.ShardObject,
        uuid: str,
        txid: int,
        partition: str,
        kb: str,
    ) -> None:
        req = noderesources_pb2.ResourceID()
        req.uuid = uuid

        nidx = get_nidx()
        if nidx is not None and shard.nidx_shard_id:
            indexpb: nodewriter_pb2.IndexMessage = nodewriter_pb2.IndexMessage()
            indexpb.shard = shard.nidx_shard_id
            indexpb.resource = uuid
            indexpb.typemessage = nodewriter_pb2.TypeMessage.DELETION
            await nidx.index(indexpb)

    async def add_resource(
        self,
        shard: writer_pb2.ShardObject,
        resource: noderesources_pb2.Resource,
        txid: int,
        partition: str,
        kb: str,
        reindex_id: Optional[str] = None,
        source: IndexMessageSource.ValueType = IndexMessageSource.PROCESSOR,
    ) -> None:
        """
        Calls the node writer's SetResource method directly to store the resource in the node.
        There is no queuing for standalone nodes at the moment -- indexing is done synchronously.
        """

        nidx = get_nidx()
        if nidx is not None and shard.nidx_shard_id:
            storage = await get_storage()
            indexpb = IndexMessage()
            storage_key = await storage.indexing(
                resource, txid, partition, kb=kb, logical_shard=shard.shard
            )

            indexpb.typemessage = TypeMessage.CREATION
            indexpb.storage_key = storage_key
            indexpb.kbid = kb
            indexpb.source = source
            indexpb.resource = resource.resource.uuid
            indexpb.shard = shard.nidx_shard_id

            await nidx.index(indexpb)

            # Delete indexing message (no longer needed)
            try:
                if storage.indexing_bucket:
                    await storage.delete_upload(storage_key, storage.indexing_bucket)
            except Exception:
                pass
