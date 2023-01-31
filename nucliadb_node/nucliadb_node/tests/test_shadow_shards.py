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
from nucliadb_protos.noderesources_pb2 import Resource

from nucliadb_node.shadow_shards import (
    AlreadyExistingShadowShard,
    OperationCode,
    ShadowShardNotFound,
    ShadowShards,
    ShadowShardsNotLoaded,
)


@pytest.fixture(scope="function")
def shadow_folder():
    with tempfile.TemporaryDirectory() as td:
        yield td


@pytest.mark.asyncio
async def test_load(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    assert not shadow.loaded
    await shadow.load()
    assert shadow.loaded
    assert len(shadow.shards) == 0

    # Create a new shadow shard and make sure it's loaded
    await shadow.create("shard1")
    await shadow.load()
    assert len(shadow.shards) == 1


@pytest.mark.asyncio
async def test_create(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        await shadow.create("foo")

    await shadow.load()
    assert len(shadow.shards) == 0
    await shadow.create("shard1")
    assert len(shadow.shards) == 1
    assert shadow.exists("shard1")

    with pytest.raises(AlreadyExistingShadowShard):
        await shadow.create("shard1")


@pytest.mark.asyncio
async def test_delete(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        await shadow.delete("foo")

    await shadow.load()
    with pytest.raises(ShadowShardNotFound):
        await shadow.delete("not-there")

    await shadow.create("shard1")
    assert len(shadow.shards) == 1
    assert shadow.exists("shard1")

    await shadow.delete("shard1")
    assert len(shadow.shards) == 0
    assert not shadow.exists("shard1")


@pytest.mark.asyncio
async def test_exists(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    with pytest.raises(ShadowShardsNotLoaded):
        shadow.exists("foo")
    await shadow.load()
    assert not shadow.exists("foo")
    await shadow.create("foo")
    assert shadow.exists("foo")


def get_brain(uuid: str) -> Resource:
    br = Resource()
    br.resource.uuid = uuid
    return br


@pytest.mark.asyncio
async def test_operations(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    shard1 = "shard1"
    shard2 = "shard2"
    shard3 = "shard3"

    await shadow.load()
    await shadow.create(shard1)
    await shadow.create(shard2)
    await shadow.create(shard3)

    brain1 = get_brain("resource1")
    brain2 = get_brain("resource2")
    brain3 = get_brain("resource3")

    await shadow.set_resource(brain1, shard1)
    await shadow.set_resource(brain2, shard1)

    await shadow.set_resource(brain3, shard2)
    await shadow.delete_resource("resource3", shard2)

    shard1_ops = [op async for op in shadow.iter_operations(shard1)]
    shard2_ops = [op async for op in shadow.iter_operations(shard2)]
    shard3_ops = [op async for op in shadow.iter_operations(shard3)]

    assert len(shard1_ops) == 2
    assert len(shard2_ops) == 2
    assert len(shard3_ops) == 0

    assert shard1_ops[0][0] == OperationCode.SET
    assert shard1_ops[0][1] == brain1
    assert shard1_ops[1][0] == OperationCode.SET
    assert shard1_ops[1][1] == brain2
    assert shard2_ops[0][0] == OperationCode.SET
    assert shard2_ops[0][1] == brain3
    assert shard2_ops[1][0] == OperationCode.DELETE
    assert shard2_ops[1][1] == "resource3"
