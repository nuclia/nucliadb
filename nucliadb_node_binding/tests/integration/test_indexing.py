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

from datetime import datetime

import pytest
from nucliadb_protos.nodereader_pb2 import (
    DocumentSearchRequest,
    DocumentSearchResponse,
    SearchRequest,
    SearchResponse,
)
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_protos.nodewriter_pb2 import NewShardRequest, OpStatus, ShardCreated
from nucliadb_protos.utils_pb2 import ReleaseChannel, VectorSimilarity

from nucliadb_node_binding import NodeReader, NodeWriter  # type: ignore


class IndexNode:
    def __init__(self):
        self.writer = NodeWriter()
        self.reader = NodeReader()

    def request(self, func, req_ob, resp_factory):
        raw_resp = func(req_ob.SerializeToString())
        resp = resp_factory()
        resp.ParseFromString(bytes(raw_resp))
        return resp

    def create_resource(self, shard_id):
        resource = Resource()
        resource.resource.uuid = "001"
        resource.resource.shard_id = shard_id
        resource.texts["field1"].text = "My lovely text"
        resource.status = Resource.ResourceStatus.PROCESSED
        resource.shard_id = shard_id
        now = datetime.now()
        resource.metadata.created.FromDatetime(now)
        resource.metadata.modified.FromDatetime(now)
        op_status = self.request(self.writer.set_resource, resource, OpStatus)
        assert op_status.status == OpStatus.OK
        return resource

    def create_shard(self, name, channel=ReleaseChannel.STABLE):
        # create a shard
        shard_req = NewShardRequest(
            kbid=name, similarity=VectorSimilarity.COSINE, release_channel=channel
        )  # NOQA
        pb = self.request(self.writer.new_shard, shard_req, ShardCreated)
        shard_id = pb.id
        assert shard_id is not None
        return shard_id

    def call_search_api(self, name, request, resp_klass):
        return self.request(getattr(self.reader, name), request, resp_klass)


@pytest.mark.asyncio
async def test_set_and_search(data_path):
    return await _test_set_and_search(ReleaseChannel.STABLE)


@pytest.mark.asyncio
async def test_set_and_search_exp(data_path):
    return await _test_set_and_search(ReleaseChannel.EXPERIMENTAL)


async def _test_set_and_search(channel):
    cluster = IndexNode()

    shard_id = cluster.create_shard("test-kbid", channel)
    cluster.create_resource(shard_id)

    # search using the generic search API
    searchpb = SearchRequest()
    searchpb.shard = shard_id
    searchpb.body = "lovely"
    searchpb.document = True
    pbresult = cluster.call_search_api("search", searchpb, SearchResponse)
    assert pbresult.document.total == 1


@pytest.mark.asyncio
async def test_set_and_document_search(data_path):
    cluster = IndexNode()
    shard_id = cluster.create_shard("test-kbid")
    cluster.create_resource(shard_id)

    # search using the document search API
    searchpb = DocumentSearchRequest()
    searchpb.id = shard_id
    searchpb.body = "lovely"
    pbresult = cluster.call_search_api(
        "document_search", searchpb, DocumentSearchResponse
    )
    assert pbresult.total == 1
