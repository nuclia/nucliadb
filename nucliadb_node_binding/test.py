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
import os
import shutil
from contextlib import contextmanager
from datetime import datetime
from tempfile import mkdtemp

from nucliadb_protos.nodereader_pb2 import SearchRequest, SearchResponse
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_protos.nodewriter_pb2 import ShardCreated, NewShardRequest
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_protos.nodewriter_pb2 import OpStatus

from nucliadb_node_binding import NodeReader, NodeWriter


@contextmanager
def temp_data_path():
    if "DATA_PATH" in os.environ:
        saved_path = os.environ["DATA_PATH"]
    else:
        saved_path = None

    dir = mkdtemp()
    try:
        os.environ["DATA_PATH"] = dir
        yield
    finally:
        shutil.rmtree(dir)
        if saved_path is not None:
            os.environ["DATA_PATH"] = saved_path
        else:
            del os.environ["DATA_PATH"]


# TODO improve coverage (see what nucliadb/common/cluster/standalone/grpc_node_binding.py does)
async def main():
    with temp_data_path():
        writer = NodeWriter()
        reader = NodeReader()
        req = NewShardRequest(kbid="test-kbid", similarity=VectorSimilarity.COSINE)

        req = req.SerializeToString()
        shard = writer.new_shard(req)

        pb = ShardCreated()
        pb.ParseFromString(bytes(shard))
        assert pb.id is not None

        resourcepb = Resource()
        resourcepb.resource.uuid = "001"
        resourcepb.resource.shard_id = pb.id
        resourcepb.texts["field1"].text = "My lovely text"
        resourcepb.status = Resource.ResourceStatus.PROCESSED
        resourcepb.shard_id = pb.id
        resourcepb.metadata.created.FromDatetime(datetime.now())
        resourcepb.metadata.modified.FromDatetime(datetime.now())

        pb_bytes = writer.set_resource(resourcepb.SerializeToString())
        op_status = OpStatus()
        op_status.ParseFromString(bytes(pb_bytes))
        assert op_status.status == OpStatus.OK

        searchpb = SearchRequest()
        searchpb.shard = pb.id
        searchpb.body = "text"
        pbresult = reader.search(searchpb.SerializeToString())
        pb = SearchResponse()
        pb.ParseFromString(bytes(pbresult))

        print(pb)


asyncio.run(main())
