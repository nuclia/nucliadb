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
from collections.abc import Awaitable, Callable
from typing import Any

from nidx_protos import nodereader_pb2, noderesources_pb2
from nidx_protos.nodewriter_pb2 import IndexMessageSource

from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import (
    NodeError,
    ShardsNotFound,
)
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_telemetry import errors

logger = logging.getLogger(__name__)


class KBShardManager:
    # TODO: move to data manager
    async def get_shards_by_kbid_inner(self, kbid: str) -> writer_pb2.Shards:
        async with datamanagers.with_ro_transaction() as txn:
            result = await datamanagers.kb.get_shards(txn, kbid=kbid)
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
            ops.append(aw(shard_obj.shard))

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
                    f"Error while applying {getattr(aw, '__name__', '<callable>')} for all shards. Other similar errors may have been shadowed.\n"
                    f"{type(result).__name__}: {result}"
                ) from result

        return results

    # TODO: move to data manager
    async def get_current_active_shard(
        self, txn: Transaction, kbid: str
    ) -> writer_pb2.ShardObject | None:
        kb_shards = await datamanagers.kb.get_shards(txn, kbid=kbid, for_update=False)
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
        *,
        prewarm_enabled: bool,
    ) -> writer_pb2.ShardObject:
        kb_shards = await datamanagers.kb.get_shards(txn, kbid=kbid, for_update=True)
        if kb_shards is None:
            msg = ("Attempting to create a shard for a KB when it has no stored shards in maindb",)
            logger.error(msg, extra={"kbid": kbid})
            raise ShardsNotFound(msg)

        shard_uuid = uuid.uuid4().hex

        shard = writer_pb2.ShardObject(shard=shard_uuid, read_only=False)
        try:
            await self.create_index_shard(kbid, shard, prewarm_enabled=prewarm_enabled)

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

        await datamanagers.kb.set(txn, kbid=kbid, shards=kb_shards)

        return shard

    async def create_index_shard(
        self, kbid: str, shard: writer_pb2.ShardObject, *, prewarm_enabled: bool = False
    ) -> None:
        # TODO(Marklogic): Create backend index storage for a logical shard.
        return None

    async def delete_index_shard(self, shard: writer_pb2.ShardObject) -> None:
        # TODO(Marklogic): Delete backend index storage for a logical shard.
        return None

    async def get_index_shard(self, shard: writer_pb2.ShardObject) -> nodereader_pb2.Shard | None:
        # TODO(Marklogic): Read backend shard statistics.
        return None

    async def rollback_shard(self, shard: writer_pb2.ShardObject):
        await self.delete_index_shard(shard)

    async def delete_resource(
        self,
        shard: writer_pb2.ShardObject,
        uuid: str,
        txid: int,
        partition: str,
        kb: str,
    ) -> None:
        # TODO(Marklogic): Delete a resource from the backend index.
        return None

    async def add_resource(
        self,
        shard: writer_pb2.ShardObject,
        resource: noderesources_pb2.Resource,
        txid: int,
        partition: str,
        kb: str,
        reindex_id: str | None = None,
        source: IndexMessageSource.ValueType = IndexMessageSource.PROCESSOR,
    ) -> None:
        # TODO(Marklogic): Write a resource to the backend index.
        return None

    async def create_vectorset(self, kbid: str, config: knowledgebox_pb2.VectorSetConfig):
        """Create a new vectorset in all KB shards."""
        # TODO(Marklogic): Create the backend vectorset index.
        return None

    async def delete_vectorset(self, kbid: str, vectorset_id: str):
        """Delete a vectorset from all KB shards"""
        # TODO(Marklogic): Delete the backend vectorset index.
        return None


class StandaloneKBShardManager(KBShardManager):
    max_ops_before_checks = 200

    def __init__(self: "StandaloneKBShardManager"):
        super().__init__()
        self._lock = asyncio.Lock()
        self._change_count: dict[tuple[str, str], int] = {}
