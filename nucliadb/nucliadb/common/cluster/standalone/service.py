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
import os

from grpc import aio  # type: ignore

from nucliadb.common.cluster.settings import settings
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.cluster.standalone import grpc_node_binding
from nucliadb.common.cluster.standalone.utils import get_self
from nucliadb_protos import standalone_pb2, standalone_pb2_grpc
from nucliadb_utils.grpc import get_traced_grpc_server


class StandaloneClusterServiceServicer(
    standalone_pb2_grpc.StandaloneClusterServiceServicer
):
    async def NodeAction(  # type: ignore
        self, request: standalone_pb2.NodeActionRequest, context
    ) -> standalone_pb2.NodeActionResponse:
        service = request.service
        action = request.action
        try:
            if service == "reader":
                request_type, _ = grpc_node_binding.READER_METHODS[action]
            elif service == "writer":
                request_type, _ = grpc_node_binding.WRITER_METHODS[action]
            else:
                raise NotImplementedError(f"Unknown type {service}")
        except KeyError:
            raise NotImplementedError(f"Unknown method for type {service}: {action}")

        index_node_action = getattr(getattr(get_self(), service), action)
        action_request = request_type()
        action_request.ParseFromString(request.payload)
        response = await index_node_action(action_request)
        return standalone_pb2.NodeActionResponse(payload=response.SerializeToString())

    async def NodeInfo(  # type: ignore
        self, request: standalone_pb2.NodeInfoRequest, context
    ) -> standalone_pb2.NodeInfoResponse:
        index_node = get_self()
        index_node.shard_count = len(
            os.listdir(os.path.join(cluster_settings.data_path, "shards"))
        )
        return standalone_pb2.NodeInfoResponse(
            id=index_node.id,
            address=index_node.address,
            shard_count=index_node.shard_count,
        )


async def start_grpc():
    aio.init_grpc_aio()

    server = get_traced_grpc_server("standalone")

    servicer = StandaloneClusterServiceServicer()
    server.add_insecure_port(f"0.0.0.0:{settings.standalone_node_port}")
    standalone_pb2_grpc.add_StandaloneClusterServiceServicer_to_server(servicer, server)

    await server.start()

    return server
