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
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import AsyncIterator

from nucliadb_protos.nodereader_pb2 import (
    DocumentItem,
    EdgeList,
    GetShardRequest,
    ParagraphItem,
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    RelationSearchRequest,
    RelationSearchResponse,
    SearchRequest,
    SearchResponse,
    StreamRequest,
    SuggestRequest,
    SuggestResponse,
)
from nucliadb_protos.noderesources_pb2 import (
    EmptyQuery,
    EmptyResponse,
    Resource,
    ResourceID,
    ShardCreated,
    ShardId,
    ShardIds,
    VectorSetID,
    VectorSetList,
)
from nucliadb_protos.noderesources_pb2 import Shard as NodeResourcesShard
from nucliadb_protos.nodewriter_pb2 import NewShardRequest, OpStatus

from ..settings import settings

logger = logging.getLogger(__name__)

try:
    from nucliadb_node_binding import IndexNodeException  # type: ignore
except ImportError:  # pragma: no cover
    logger.warning("Import error while importing IndexNodeException")
    IndexNodeException = Exception

try:
    from nucliadb_node_binding import NodeReader, NodeWriter
except ImportError:  # pragma: no cover
    NodeReader = None
    NodeWriter = None


class StandaloneReaderWrapper:
    reader: NodeReader

    def __init__(self):
        if NodeReader is None:
            raise ImportError("NucliaDB index node bindings are not installed (reader not found)")
        self.reader = NodeReader()
        self.executor = ThreadPoolExecutor(settings.local_reader_threads)

    async def Search(self, request: SearchRequest, retry: bool = False) -> SearchResponse:
        try:
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                self.executor, self.reader.search, request.SerializeToString()
            )
            pb_bytes = bytes(result)
            pb = SearchResponse()
            pb.ParseFromString(pb_bytes)
            return pb
        except IndexNodeException as exc:
            if "IO error" not in str(exc):
                # ignore any other error
                raise

            # try some mitigations...
            logger.error(f"IndexNodeException in Search: {request}", exc_info=True)
            if not retry:
                # reinit?
                self.reader = NodeReader()
                return await self.Search(request, retry=True)
            else:
                raise

    async def ParagraphSearch(self, request: ParagraphSearchRequest) -> ParagraphSearchResponse:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.paragraph_search, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        pb = ParagraphSearchResponse()
        pb.ParseFromString(pb_bytes)
        return pb

    async def RelationSearch(self, request: RelationSearchRequest) -> RelationSearchResponse:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.relation_search, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        pb = RelationSearchResponse()
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
    ) -> AsyncIterator[DocumentItem]:  # pragma: no cover
        """
        This is a workaround for the fact that the node binding does not support async generators.

        Very difficult to write tests for
        """
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
            except StopIteration:
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

    async def Paragraphs(self, stream_request: StreamRequest) -> AsyncIterator[ParagraphItem]:
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
            except StopIteration:
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

    async def RelationEdges(self, request: ShardId):
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.relation_edges, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        edge_list = EdgeList()
        edge_list.ParseFromString(pb_bytes)
        return edge_list


async def Search(self, request: SearchRequest, retry: bool = False) -> SearchResponse:
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            self.executor, self.reader.search, request.SerializeToString()
        )
        pb_bytes = bytes(result)
        pb = SearchResponse()
        pb.ParseFromString(pb_bytes)
        return pb
    except IndexNodeException as exc:
        if "IO error" not in str(exc):
            # ignore any other error
            raise

        # try some mitigations...
        logger.error(f"IndexNodeException in Search: {request}", exc_info=True)
        if not retry:
            # reinit?
            self.reader = NodeReader()
            return await self.Search(request, retry=True)
        else:
            raise


class StandaloneWriterWrapper:
    writer: NodeWriter

    def __init__(self):
        os.makedirs(settings.data_path, exist_ok=True)
        if NodeWriter is None:
            raise ImportError("NucliaDB index node bindings are not installed (writer not found)")
        self.writer = NodeWriter()
        self.executor = ThreadPoolExecutor(settings.local_writer_threads)

    async def NewShard(self, request: NewShardRequest) -> ShardCreated:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.new_shard, request.SerializeToString()
        )
        pb_bytes = bytes(resp)
        shard_created = ShardCreated()
        shard_created.ParseFromString(pb_bytes)
        return shard_created

    async def DeleteShard(self, request: ShardId) -> ShardId:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.delete_shard, request.SerializeToString()
        )
        pb_bytes = bytes(resp)
        shard_id = ShardId()
        shard_id.ParseFromString(pb_bytes)
        return shard_id

    async def ListShards(self, request: EmptyQuery) -> ShardIds:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor,
            self.writer.list_shards,
        )
        pb_bytes = bytes(resp)
        shard_ids = ShardIds()
        shard_ids.ParseFromString(pb_bytes)
        return shard_ids

    async def AddVectorSet(self, request: VectorSetID):
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.add_vectorset, request.SerializeToString()
        )
        pb_bytes = bytes(resp)
        resp = OpStatus()
        resp.ParseFromString(pb_bytes)
        return resp

    async def ListVectorSets(self, request: ShardId):
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.list_vectorsets, request.SerializeToString()
        )
        pb_bytes = bytes(resp)
        resp = VectorSetList()
        resp.ParseFromString(pb_bytes)
        return resp

    async def RemoveVectorSet(self, request: VectorSetID):
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.remove_vectorset, request.SerializeToString()
        )
        pb_bytes = bytes(resp)
        resp = OpStatus()
        resp.ParseFromString(pb_bytes)
        return resp

    async def SetResource(self, request: Resource) -> OpStatus:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.set_resource, request.SerializeToString()
        )
        pb_bytes = bytes(resp)
        op_status = OpStatus()
        op_status.ParseFromString(pb_bytes)
        return op_status

    async def RemoveResource(self, request: ResourceID) -> OpStatus:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(
            self.executor, self.writer.remove_resource, request.SerializeToString()
        )
        pb_bytes = bytes(resp)
        op_status = OpStatus()
        op_status.ParseFromString(pb_bytes)
        return op_status

    async def GC(self, request: ShardId) -> EmptyResponse:
        loop = asyncio.get_running_loop()
        resp = await loop.run_in_executor(self.executor, self.writer.gc, request.SerializeToString())
        pb_bytes = bytes(resp)
        op_status = EmptyResponse()
        op_status.ParseFromString(pb_bytes)
        return op_status


# supported marshalled reader methods for standalone node support
READER_METHODS = {
    "Search": (SearchRequest, SearchResponse),
    "ParagraphSearch": (ParagraphSearchRequest, ParagraphSearchResponse),
    "RelationSearch": (RelationSearchRequest, RelationSearchResponse),
    "GetShard": (GetShardRequest, NodeResourcesShard),
    "Suggest": (SuggestRequest, SuggestResponse),
    "RelationEdges": (ShardId, EdgeList),
}
WRITER_METHODS = {
    "NewShard": (NewShardRequest, ShardCreated),
    "DeleteShard": (ShardId, ShardId),
    "ListShards": (EmptyQuery, ShardIds),
    "RemoveVectorSet": (VectorSetID, OpStatus),
    "AddVectorSet": (VectorSetID, OpStatus),
    "ListVectorSets": (ShardId, VectorSetList),
    "SetResource": (Resource, OpStatus),
    "RemoveResource": (ResourceID, OpStatus),
    "GC": (ShardId, EmptyResponse),
}
