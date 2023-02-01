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
    ShadowShards,
    ShadowShardsNotLoaded,
    get_shadow_shards,
)


@pytest.mark.asyncio
async def test_get_shadow_shards(shadow_folder):
    shadow_shards = get_shadow_shards()
    assert isinstance(shadow_shards, ShadowShards)
    assert shadow_shards._folder == f"{shadow_folder}/shadow_shards/"


@pytest.mark.asyncio
async def test_load(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    assert not shadow.loaded
    await shadow.load()
    assert shadow.loaded
    assert len(shadow.shards) == 0

    # Create a new shadow shard and make sure it's loaded
    shard_id = await shadow.create()
    assert shard_id
    await shadow.load()
    assert len(shadow.shards) == 1


@pytest.mark.asyncio
async def test_create(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        await shadow.create()

    await shadow.load()
    assert len(shadow.shards) == 0

    shard_id = await shadow.create()
    assert len(shadow.shards) == 1
    assert shadow.exists(shard_id)


@pytest.mark.asyncio
async def test_delete(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        await shadow.delete("foo")

    await shadow.load()
    with pytest.raises(ShadowShardNotFound):
        await shadow.delete("not-there")

    shard_id = await shadow.create()
    assert len(shadow.shards) == 1
    assert shadow.exists(shard_id)

    await shadow.delete(shard_id)
    assert len(shadow.shards) == 0
    assert not shadow.exists(shard_id)


@pytest.mark.asyncio
async def test_exists(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        shadow.exists("foo")
    await shadow.load()
    assert not shadow.exists("foo")
    shard_id = await shadow.create()
    assert shadow.exists(shard_id)


def get_brain(uuid: str) -> Resource:
    br = Resource()
    br.resource.uuid = uuid
    return br


@pytest.mark.asyncio
async def test_operations(shadow_folder):
    shadow = ShadowShards(shadow_folder)

    await shadow.load()
    shard_1 = await shadow.create()
    shard_2 = await shadow.create()
    shard_3 = await shadow.create()

    brain_1 = get_brain("resource1")
    brain_2 = get_brain("resource2")
    brain_3 = get_brain("resource3")

    await shadow.set_resource(brain_1, shard_1)
    await shadow.set_resource(brain_2, shard_1)

    await shadow.set_resource(brain_3, shard_2)
    await shadow.delete_resource("resource3", shard_2)

    shard1_ops = [op async for op in shadow.iter_operations(shard_1)]
    shard2_ops = [op async for op in shadow.iter_operations(shard_2)]
    shard3_ops = [op async for op in shadow.iter_operations(shard_3)]

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
    shadow = ShadowShards(shadow_folder)

    # Check that error is thrown if not loaded
    with pytest.raises(ShadowShardsNotLoaded):
        shadow.metadata

    with pytest.raises(ShadowShardsNotLoaded):
        await shadow.save_metadata()

    # Check initial value
    await shadow.load()
    assert shadow.metadata.shards == {}

    # Check initial shardinfo data
    shard_1 = await shadow.create()

    assert shadow.metadata.get_info(shard_1).created_at
    assert shadow.metadata.get_info(shard_1).modified_at
    assert shadow.metadata.get_info(shard_1).operations == 0

    # Check operations are incremented
    await shadow.set_resource(Resource(), shard_1)
    await shadow.delete_resource("foo", shard_1)

    assert shadow.metadata.get_info(shard_1).operations == 2

    # Check that it has been persisted in disk
    shadow = ShadowShards(shadow_folder)
    await shadow.load()
    assert shadow.metadata.get_info(shard_1).operations == 2

    # Check that deleting the shard cleans up the metadata
    await shadow.delete(shard_1)
    assert shadow.metadata.get_info(shard_1) is None
