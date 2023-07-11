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
from nucliadb_protos import nodereader_pb2, noderesources_pb2, nodesidecar_pb2

from ..abc import AbstractIndexNode
from .grpc_node_binding import StandaloneReaderWrapper, StandaloneWriterWrapper


class StandaloneSidecarInterface:
    """
    backward compatibile interface for sidecar
    type interactions when running standalone.

    Right now, side car only provides cached counters.

    Long term, this should be removed and any caching
    should be done at the node reader.
    """

    def __init__(self, reader: StandaloneReaderWrapper):
        self._reader = reader

    async def GetCount(
        self, shard_id: noderesources_pb2.ShardId
    ) -> nodesidecar_pb2.Counter:
        shard = await self._reader.GetShard(
            nodereader_pb2.GetShardRequest(shard_id=shard_id)
        )
        response = nodesidecar_pb2.Counter()
        if shard is not None:
            response.fields = shard.fields
            response.paragraphs = shard.paragraphs
        return response


class StandaloneIndexNode(AbstractIndexNode):
    _writer: StandaloneWriterWrapper
    _reader: StandaloneReaderWrapper
    label: str = "standalone"

    def __init__(self, id: str, address: str, shard_count: int, dummy: bool = False):
        super().__init__(id=id, address=address, shard_count=shard_count, dummy=dummy)
        self._writer = StandaloneWriterWrapper()
        self._reader = StandaloneReaderWrapper()
        self._sidecar = StandaloneSidecarInterface(self._reader)

    @property
    def reader(self) -> StandaloneReaderWrapper:  # type: ignore
        return self._reader

    @property
    def writer(self) -> StandaloneWriterWrapper:  # type: ignore
        return self._writer

    @property
    def sidecar(self) -> StandaloneSidecarInterface:  # type: ignore
        return self._sidecar
