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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.nodereader_pb2 import Shard, ShardId

from nucliadb_node.reader import CACHE, Reader


@pytest.fixture()
def shard():
    yield Shard()


@pytest.fixture()
def stub(shard):
    mock = MagicMock()
    mock.GetShard = AsyncMock(return_value=shard)
    with patch("nucliadb_node.reader.get_traced_grpc_channel"), patch(
        "nucliadb_node.reader.NodeReaderStub", return_value=mock
    ):
        yield mock


@pytest.fixture()
def reader(stub):
    CACHE.clear()
    yield Reader("address")


@pytest.mark.asyncio
async def test_get_shard(reader, stub, shard):
    shard_id = ShardId(id="shard_id")
    assert await reader.get_shard(shard_id) == shard

    stub.GetShard.assert_awaited_once()

    # next call from cache
    assert await reader.get_shard(shard_id) == shard

    assert stub.GetShard.await_count == 1


@pytest.mark.asyncio
async def test_get_shard_not_found(reader, stub, shard):
    stub.GetShard.side_effect = AioRpcError(
        code=StatusCode.NOT_FOUND,
        initial_metadata=MagicMock(),
        trailing_metadata=MagicMock(),
        details="not found",
        debug_error_string="",
    )

    shard_id = ShardId(id="shard_id")
    assert await reader.get_shard(shard_id) is None

    stub.GetShard.assert_awaited_once()
