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

import pytest
from nucliadb_protos.noderesources_pb2 import EmptyQuery, ShardId
from nucliadb_protos.nodewriter_pb2_grpc import NodeSidecarStub


@pytest.mark.asyncio
async def test_create_delete_shadow_shards(shadow_folder, sidecar_grpc_servicer):
    stub = NodeSidecarStub(sidecar_grpc_servicer)

    # Create a shadow shard
    response = await stub.CreateShadowShard(EmptyQuery())
    assert response.success

    # Delete now
    sipb = ShardId(id=response.shard.id)
    response = await stub.DeleteShadowShard(sipb)
    assert response.success

    # Deleting again should succed (not found)
    response = await stub.DeleteShadowShard(sipb)
    assert response.success
