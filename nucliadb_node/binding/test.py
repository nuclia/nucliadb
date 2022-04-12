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
from datetime import datetime

import nucliadb_node_binding  # type: ignore
from nucliadb_protos.nodereader_pb2 import SearchRequest, SearchResponse
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_protos.nodewriter_pb2 import ShardCreated


async def main():
    writer = nucliadb_node_binding.NodeWriter.new()
    reader = nucliadb_node_binding.NodeReader.new()
    shard = await writer.new_shard()
    pb = ShardCreated()
    pb.ParseFromString(bytearray(shard))

    resourcepb = Resource()
    resourcepb.resource.uuid = "001"
    resourcepb.resource.shard_id = pb.id
    resourcepb.texts["field1"].text = "My lovely text"
    resourcepb.status = Resource.ResourceStatus.PROCESSED
    resourcepb.shard_id = pb.id
    resourcepb.metadata.created.FromDatetime(datetime.now())
    resourcepb.metadata.modified.FromDatetime(datetime.now())
    await writer.set_resource(resourcepb.SerializeToString())

    searchpb = SearchRequest()
    searchpb.shard = pb.id
    searchpb.body = "text"
    pbresult = await reader.search(searchpb.SerializeToString())
    pb = SearchResponse()
    pb.ParseFromString(bytearray(pbresult))

    print(pb)


asyncio.run(main())
