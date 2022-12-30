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
from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncIterator, Optional
from uuid import uuid4

from nucliadb_protos.nodereader_pb2 import (
    DocumentItem,
    GetShardRequest,
    ParagraphItem,
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    SearchRequest,
    SearchResponse,
    StreamRequest,
    SuggestRequest,
    SuggestResponse,
)
from nucliadb_protos.noderesources_pb2 import Resource, ResourceID
from nucliadb_protos.noderesources_pb2 import Shard as NodeResourcesShard
from nucliadb_protos.noderesources_pb2 import (
    ShardCleaned,
    ShardCreated,
    ShardId,
    ShardIds,
    VectorSetID,
    VectorSetList,
)
from nucliadb_protos.nodewriter_pb2 import OpStatus
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm import NODE_CLUSTER
from nucliadb.ingest.orm.abc import AbstractNode
from nucliadb.ingest.orm.local_shard import LocalShard
from nucliadb.ingest.settings import settings
from nucliadb_utils.keys import KB_SHARDS

try:
    from nucliadb_node_binding import NodeReader  # type: ignore
    from nucliadb_node_binding import NodeWriter  # type: ignore
except ImportError:
    NodeWriter = None
    NodeReader = None


class LocalReaderWrapper:
    reader: NodeReader

    def __init__(self):
        self.reader = NodeReader.new()
        self.executor = ThreadPoolExecutor(settings.local_reader_threads)

    async def Search(self, request: SearchRequest) -> SearchResponse:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.search, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        pb = SearchResponse()
        pb.ParseFromString(pb_bytes)
        return pb

    async def ParagraphSearch(
        self, request: ParagraphSearchRequest
    ) -> ParagraphSearchResponse:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.paragraph_search, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        pb = ParagraphSearchResponse()
        pb.ParseFromString(pb_bytes)
        return pb

    async def GetShard(self, request: GetShardRequest) -> NodeResourcesShard:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.get_shard, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        shard = NodeResourcesShard()
        shard.ParseFromString(pb_bytes)
        return shard

    async def Suggest(self, request: SuggestRequest) -> SuggestResponse:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.suggest, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        pb = SuggestResponse()
        pb.ParseFromString(pb_bytes)
        return pb

    async def Documents(
        self, stream_request: StreamRequest
    ) -> AsyncIterator[DocumentItem]:
        loop = asyncio.get_running_loop()
        q: asyncio.Queue[DocumentItem] = asyncio.Queue(1)
        exception = None
        _END = object()

        def thread_generator():
            nonlocal exception
            generator = self.reader.documents(stream_request.SerializeToString())
            try:
                element = generator.next()
                while element is not None:
                    pb_bytes = bytes(element)
                    pb = DocumentItem()
                    pb.ParseFromString(pb_bytes)
                    asyncio.run_coroutine_threadsafe(q.put(pb), loop).result()
                    element = generator.next()
            except TypeError:
                # this is the end
                pass
            except Exception as e:
                exception = e
            finally:
                asyncio.run_coroutine_threadsafe(q.put(_END), loop).result()

        t1 = threading.Thread(target=thread_generator)
        t1.start()

        while True:
            next_item = await q.get()
            if next_item is _END:
                break
            yield next_item
        if exception is not None:
            raise exception
        await loop.run_in_executor(self.executor, t1.join)

    async def Paragraphs(
        self, stream_request: StreamRequest
    ) -> AsyncIterator[ParagraphItem]:

        loop = asyncio.get_running_loop()
        q: asyncio.Queue[ParagraphItem] = asyncio.Queue(1)
        exception = None
        _END = object()

        def thread_generator():
            nonlocal exception
            generator = self.reader.paragraphs(stream_request.SerializeToString())
            try:
                element = generator.next()
                while element is not None:
                    pb_bytes = bytes(element)
                    pb = ParagraphItem()
                    pb.ParseFromString(pb_bytes)
                    asyncio.run_coroutine_threadsafe(q.put(pb), loop).result()
                    element = generator.next()
            except TypeError:
                # this is the end
                pass
            except Exception as e:
                exception = e
            finally:
                asyncio.run_coroutine_threadsafe(q.put(_END), loop).result()

        t1 = threading.Thread(target=thread_generator)
        t1.start()
        while True:
            next_item = await q.get()
            if next_item is _END:
                break
            yield next_item
        if exception is not None:
            raise exception
        await loop.run_in_executor(self.executor, t1.join)


class LocalNode(AbstractNode):
    writer: NodeWriter
    reader: LocalReaderWrapper
    label: str = "local"

    def __init__(self):
        self.writer = NodeWriter.new()
        self.reader = LocalReaderWrapper()
        self.address = "local"
        self.executor = ThreadPoolExecutor(settings.local_writer_threads)

    @classmethod
    async def get(cls, _) -> LocalNode:
        return NODE_CLUSTER.get_local_node()

    @classmethod
    async def create_shard_by_kbid(cls, txn: Transaction, kbid: str) -> LocalShard:
        node = NODE_CLUSTER.get_local_node()
        sharduuid = uuid4().hex
        shard = PBShard(shard=sharduuid)
        shard_created = await node.new_shard()
        sr = ShardReplica(node=str(node))
        sr.shard.CopyFrom(shard_created)
        shard.replicas.append(sr)

        key = KB_SHARDS.format(kbid=kbid)
        payload = await txn.get(key)
        kb_shards = PBShards()
        if payload is not None:
            kb_shards.ParseFromString(payload)
        else:
            kb_shards.kbid = kbid
            kb_shards.actual = -1
        kb_shards.shards.append(shard)
        kb_shards.actual += 1
        await txn.set(key, kb_shards.SerializeToString())

        return LocalShard(sharduuid=sharduuid, shard=shard, node=node)

    @classmethod
    def create_shard_klass(cls, shard_id: str, pbshard: PBShard):
        node = NODE_CLUSTER.get_local_node()
        return LocalShard(sharduuid=shard_id, shard=pbshard, node=node)

    @classmethod
    async def actual_shard(cls, txn: Transaction, kbid: str) -> Optional[LocalShard]:
        key = KB_SHARDS.format(kbid=kbid)
        kb_shards_bytes: Optional[bytes] = await txn.get(key)
        if kb_shards_bytes is not None:
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_bytes)
            shard: PBShard = kb_shards.shards[kb_shards.actual]
            node = NODE_CLUSTER.get_local_node()
            return LocalShard(sharduuid=shard.shard, shard=shard, node=node)
        else:
            return None

    @classmethod
    async def get_all_shards(cls, txn: Transaction, kbid: str) -> Optional[PBShards]:
        key = KB_SHARDS.format(kbid=kbid)
        kb_shards_bytes: Optional[bytes] = await txn.get(key)
        if kb_shards_bytes is not None:
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_bytes)
            return kb_shards
        else:
            return None

    async def get_shard(self, id: str) -> ShardId:
        req = ShardId(id=id)
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.get_shard, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        shard_id = ShardId()
        shard_id.ParseFromString(pb_bytes)

        return shard_id

    async def get_reader_shard(
        self, shard_id: str, vectorset: Optional[str] = None
    ) -> NodeResourcesShard:
        req = GetShardRequest()
        req.shard_id.id = shard_id
        if vectorset is not None:
            req.vectorset = vectorset
        return await self.reader.GetShard(req)  # type: ignore

    async def new_shard(self) -> ShardCreated:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(self.executor, self.writer.new_shard)
        pb_bytes = bytes(resp)
        shard_created = ShardCreated()
        shard_created.ParseFromString(pb_bytes)
        return shard_created

    async def delete_shard(self, id: str) -> str:
        req = ShardId(id=id)
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.delete_shard, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        shard_id = ShardId()
        shard_id.ParseFromString(pb_bytes)

        return shard_id.id

    async def list_shards(self) -> ShardIds:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(self.executor, self.writer.list_shards)
        pb_bytes = bytes(resp)
        shards_ids = ShardIds()
        shards_ids.ParseFromString(pb_bytes)
        return shards_ids

    async def add_resource(self, req: Resource) -> OpStatus:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.set_resource, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        op_status = OpStatus()
        op_status.ParseFromString(pb_bytes)
        return op_status

    async def delete_resource(self, req: ResourceID) -> OpStatus:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.remove_resource, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        op_status = OpStatus()
        op_status.ParseFromString(pb_bytes)
        return op_status

    async def clean_and_upgrade_shard(self, shard_id: str) -> ShardCleaned:
        loop = asyncio.get_running_loop()
        req = ShardId(id=shard_id)
        resp = await loop.run_in_executor(
            self.executor, self.writer.clean_and_upgrade_shard, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        resp = ShardCleaned()
        resp.ParseFromString(pb_bytes)
        return resp

    async def del_vectorset(self, shard_id: str, vectorset: str):
        loop = asyncio.get_running_loop()
        req = VectorSetID()
        req.shard.id = shard_id
        req.vectorset = vectorset
        resp = await loop.run_in_executor(
            self.executor, self.writer.del_vectorset, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        resp = OpStatus()
        resp.ParseFromString(pb_bytes)
        return resp

    async def set_vectorset(self, shard_id: str, vectorset: str):
        loop = asyncio.get_running_loop()
        req = VectorSetID()
        req.shard.id = shard_id
        req.vectorset = vectorset
        resp = await loop.run_in_executor(
            self.executor, self.writer.set_vectorset, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        resp = OpStatus()
        resp.ParseFromString(pb_bytes)
        return resp

    async def get_vectorset(self, shard_id: str):
        loop = asyncio.get_running_loop()
        req = ShardId()
        req.id = shard_id
        resp = await loop.run_in_executor(
            self.executor, self.writer.get_vectorset, req.SerializeToString()
        )
        pb_bytes = bytes(resp)
        resp = VectorSetList()
        resp.ParseFromString(pb_bytes)
        return resp

    def __str__(self):
        return "LOCAL NODE"

    def __repr__(self):
        return "LOCAL NODE"
