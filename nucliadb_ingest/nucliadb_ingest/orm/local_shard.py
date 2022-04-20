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

from typing import TYPE_CHECKING

from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.noderesources_pb2 import ResourceID
from nucliadb_protos.writer_pb2 import ShardObject as PBShard

if TYPE_CHECKING:
    from nucliadb_ingest.orm.local_node import LocalNode


class LocalShard:
    def __init__(self, sharduuid: str, shard: PBShard, node: LocalNode):
        self.shard = shard
        self.sharduuid = sharduuid
        self.node = node

    async def delete_resource(self, uuid: str, txid: int):
        req = ResourceID()
        req.uuid = uuid
        req.shard_id = self.sharduuid
        self.node.delete_resource(req)

    async def add_resource(self, resource: PBBrainResource, txid: int) -> int:
        res = await self.node.add_resource(resource)
        return res.count
