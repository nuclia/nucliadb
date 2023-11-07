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
from uuid import uuid4

import pytest

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.utils import get_driver
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_create_cleansup_on_error(grpc_servicer, fake_node):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)
    # Create a KB
    kbid = str(uuid4())
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test", forceuuid=kbid)
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    # Get current shards object
    driver = get_driver()
    shard_manager = KBShardManager()
    txn = await driver.begin()
    shards_object = await shard_manager.get_all_shards(txn, kbid)
    await txn.abort()
    assert shards_object
