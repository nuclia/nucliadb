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

from nucliadb_protos.noderesources_pb2 import EmptyQuery, ShardId
from nucliadb_protos.nodesidecar_pb2 import Counter, ShadowShardResponse

from nucliadb_node import logger, shadow_shards
from nucliadb_node.reader import Reader
from nucliadb_node.shadow_shards import ShadowShardNotFound, ShadowShardsManager
from nucliadb_node.writer import Writer
from nucliadb_protos import nodesidecar_pb2_grpc
from nucliadb_telemetry import errors


class SidecarServicer(nodesidecar_pb2_grpc.NodeSidecarServicer):
    def __init__(self, reader: Reader, writer: Writer):
        self.reader = reader
        self.writer = writer

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    async def GetCount(self, request: ShardId, context) -> Counter:  # type: ignore
        response = Counter()
        shard = await self.reader.get_shard(request)
        if shard is not None:
            response.resources = shard.resources
            response.paragraphs = shard.paragraphs
        return response

    async def CreateShadowShard(self, request: EmptyQuery, context) -> ShadowShardResponse:  # type: ignore
        ssm: ShadowShardsManager = shadow_shards.get_manager()
        await ssm.load()
        response = ShadowShardResponse()
        try:
            shard_id = await ssm.create()
            response.success = True
            response.shard.id = shard_id
        except Exception as exc:
            errors.capture_exception(exc)
            logger.warning(f"Error creating shadow shard: {shard_id}")
        finally:
            return response

    async def DeleteShadowShard(self, request: ShardId, context) -> ShadowShardResponse:  # type: ignore
        ssm = shadow_shards.get_manager()
        await ssm.load()
        response = ShadowShardResponse()
        shard_id = request.id
        try:
            await ssm.delete(shard_id=shard_id)
            response.success = True
            response.shard.id = shard_id
        except ShadowShardNotFound:
            logger.warning(
                f"Attempting to delete a shadow shard that does not exist: {shard_id}"
            )
            response.success = True
        except Exception as exc:
            errors.capture_exception(exc)
            logger.warning(f"Error deleting shadow shard: {shard_id}")
        finally:
            return response
