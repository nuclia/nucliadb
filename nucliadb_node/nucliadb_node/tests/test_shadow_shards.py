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
from nucliadb_protos.noderesources_pb2 import Resource

from nucliadb_node.shadow_shards import (
    OperationCode,
    ShadowShardNotFound,
    ShadowShardsManager,
    ShadowShardsNotLoaded,
    get_shadow_shards_manager,
)


@pytest.mark.asyncio
async def test_get_shadow_shards_manager(shadow_folder):
    shadow_shards = get_shadow_shards_manager()
    assert isinstance(shadow_shards, ShadowShardsManager)
    assert not shadow_shards._folder.endswith("{data_path}")
    assert shadow_shards._folder.endswith("/shadow_shards/")


@pytest.mark.asyncio
async def test_load(shadow_folder):
    ssm = ShadowShardsManager(shadow_folder)
    assert not ssm.loaded
    await ssm.load()
    assert ssm.loaded
    assert len(ssm.shards) == 0

    # Create a new shadow shard and make sure it's loaded
    shard_id = await ssm.create()
    assert shard_id
    await ssm.load()
    assert len(ssm.shards) == 1


@pytest.mark.asyncio
async def test_create(shadow_folder):
    ssm = ShadowShardsManager(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        await ssm.create()

    await ssm.load()
    assert len(ssm.shards) == 0

    shard_id = await ssm.create()
    assert len(ssm.shards) == 1
    assert ssm.exists(shard_id)


@pytest.mark.asyncio
async def test_delete(shadow_folder):
    ssm = ShadowShardsManager(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        await ssm.delete("foo")

    await ssm.load()
    with pytest.raises(ShadowShardNotFound):
        await ssm.delete("not-there")

    shard_id = await ssm.create()
    assert len(ssm.shards) == 1
    assert ssm.exists(shard_id)

    await ssm.delete(shard_id)
    assert len(ssm.shards) == 0
    assert not ssm.exists(shard_id)


@pytest.mark.asyncio
async def test_exists(shadow_folder):
    ssm = ShadowShardsManager(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        ssm.exists("foo")
    await ssm.load()
    assert not ssm.exists("foo")
    shard_id = await ssm.create()
    assert ssm.exists(shard_id)


def get_brain(uuid: str) -> Resource:
    br = Resource()
    br.resource.uuid = uuid
    return br


@pytest.mark.asyncio
async def test_operations(shadow_folder):
    ssm = ShadowShardsManager(shadow_folder)

    await ssm.load()
    shard_1 = await ssm.create()
    shard_2 = await ssm.create()
    shard_3 = await ssm.create()

    brain_1 = get_brain("resource1")
    brain_2 = get_brain("resource2")
    brain_3 = get_brain("resource3")

    await ssm.set_resource(brain_2, shard_1, 10)
    await ssm.set_resource(brain_1, shard_1, 2)

    await ssm.set_resource(brain_3, shard_2, 2)
    await ssm.delete_resource("resource3", shard_2, 3)

    shard1_ops = [op async for op in ssm.iter_operations(shard_1)]
    shard2_ops = [op async for op in ssm.iter_operations(shard_2)]
    shard3_ops = [op async for op in ssm.iter_operations(shard_3)]

    assert len(shard1_ops) == 2
    assert len(shard2_ops) == 2
    assert len(shard3_ops) == 0

    assert shard1_ops[0][0] == OperationCode.SET
    assert shard1_ops[0][1] == brain_1
    assert shard1_ops[1][0] == OperationCode.SET
    assert shard1_ops[1][1] == brain_2

    assert shard2_ops[0][0] == OperationCode.SET
    assert shard2_ops[0][1] == brain_3
    assert shard2_ops[1][0] == OperationCode.DELETE
    assert shard2_ops[1][1] == "resource3"


@pytest.mark.asyncio
async def test_metadata(shadow_folder):
    ssm = ShadowShardsManager(shadow_folder)

    # Check that error is thrown if not loaded
    with pytest.raises(ShadowShardsNotLoaded):
        ssm.metadata

    with pytest.raises(ShadowShardsNotLoaded):
        await ssm.save_metadata()

    # Check initial value
    await ssm.load()
    assert ssm.metadata.shards == {}

    # Check initial shardinfo data
    shard_1 = await ssm.create()

    assert ssm.metadata.get_info(shard_1).created_at

    # Check that it has been persisted in disk
    ssm = ShadowShardsManager(shadow_folder)
    await ssm.load()
    assert ssm.metadata.get_info(shard_1).created_at

    # Check that deleting the shard cleans up the metadata
    await ssm.delete(shard_1)
    assert ssm.metadata.get_info(shard_1) is None
