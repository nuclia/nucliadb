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

import tempfile

import pytest
from nucliadb_protos.noderesources_pb2 import ShardId
from nucliadb_protos.nodewriter_pb2_grpc import NodeSidecarStub

from nucliadb_node.shadow_shards import SHADOW_SHARDS


@pytest.fixture(scope="function")
def shadow_shards_tmp_folder():
    with tempfile.TemporaryDirectory() as tmp_dir:
        previous_folder = SHADOW_SHARDS.folder
        SHADOW_SHARDS.folder = tmp_dir
        yield
        SHADOW_SHARDS.folder = previous_folder


@pytest.mark.asyncio
async def test_create_delete_shadow_shards(
    sidecar_grpc_servicer, shadow_shards_tmp_folder
):
    stub = NodeSidecarStub(sidecar_grpc_servicer)
    sipb = ShardId(id="foo")

    # Create a shadow shard
    response = await stub.ShadowShardCreate(sipb)
    assert response.success

    # Creating again should not succeed
    response = await stub.ShadowShardCreate(sipb)
    assert not response.success

    # Delete now
    response = await stub.ShadowShardDelete(sipb)
    assert response.success

    # Deleting again should succed (not found)
    response = await stub.ShadowShardDelete(sipb)
    assert response.success
