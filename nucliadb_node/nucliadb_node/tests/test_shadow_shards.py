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

from nucliadb_node.nucliadb_node.shadow_shards import (
    AlreadyExistingShadowShard,
    ShadowShardNotFound,
)
from nucliadb_node.shadow_shards import ShadowShards


@pytest.fixture(scope="function")
def shadow_folder():
    with tempfile.TemporaryDirectory() as td:
        yield td


@pytest.mark.asyncio
async def test_create_and_delete_shadow_shards(shadow_folder):
    shadow = ShadowShards(shadow_folder)
    assert not shadow.loaded
    await shadow.load()
    assert len(shadow.shards) == 0

    with pytest.raises(ShadowShardNotFound):
        await shadow.delete("not-there")

    await shadow.create("shard1")
    assert shadow.exists("shadow1")

    shadow = ShadowShards(shadow_folder)
    await shadow.load()
    assert shadow.exists("shadow1")
    assert len(shadow.shards) == 1
    shadow.clear()
    assert not shadow.loaded
    assert len(shadow.shards) == 0

    await shadow.load()
    await shadow.delete("shadow1")
    assert len(shadow.shards) == 0

    shadow = ShadowShards(shadow_folder)
    await shadow.load()
    assert not shadow.exists("shadow1")
