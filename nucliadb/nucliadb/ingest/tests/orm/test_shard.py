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

from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
import sentry_sdk
from nucliadb_protos.noderesources_pb2 import Resource as PBResource
from nucliadb_protos.nodewriter_pb2 import Counter
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica as PBShardReplica

from nucliadb.ingest.orm.shard import Shard
from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.utilities import Utility, clean_utility, set_utility


@pytest.mark.asyncio
async def test_shard_unsynced(mocker):
    from nucliadb.ingest.orm.shard import NODES

    # Setup dummy utilities, no need for real ones here
    indexing_utility = IndexingUtility(nats_servers=[], nats_target="", dummy=True)
    indexing_utility.index = AsyncMock()
    set_utility(Utility.INDEXING, indexing_utility)

    storage_util = LocalStorage(local_testing_files="")
    storage_util.indexing = AsyncMock()
    set_utility(Utility.STORAGE, storage_util)

    NODES["node1"] = Mock(
        sidecar=Mock(
            GetCount=AsyncMock(return_value=Counter(resources=2, paragraphs=2))
        )
    )
    NODES["node2"] = Mock(
        sidecar=Mock(
            GetCount=AsyncMock(return_value=Counter(resources=1, paragraphs=1))
        )
    )

    mocker.patch.object(sentry_sdk, "capture_message")
    # Create a fake shard with two replicas that will use
    # the dummy utilities when calling add_resource
    pbshard = PBShard()

    shard_id = str(uuid4())

    replica1 = PBShardReplica()
    replica1.node = "node1"
    replica1.shard.id = str(uuid4())

    replica2 = PBShardReplica()
    replica2.node = "node2"
    replica2.shard.id = str(uuid4())

    pbshard.replicas.append(replica1)
    pbshard.replicas.append(replica2)

    shard = Shard(shard_id, pbshard)

    resource = PBResource()
    resource.resource.shard_id = shard_id

    # Test we report when 2 replicas differ
    await shard.add_resource(resource, 1)
    assert sentry_sdk.capture_message.call_count == 1

    # Test we don't report when replicas match
    sentry_sdk.capture_message.reset_mock()
    NODES["node2"] = Mock(
        sidecar=Mock(
            GetCount=AsyncMock(return_value=Counter(resources=2, paragraphs=2))
        )
    )
    await shard.add_resource(resource, 1)
    assert sentry_sdk.capture_message.call_count == 0

    # Test we report when >2 replicas differ

    replica3 = PBShardReplica()
    replica3.node = "node3"
    replica3.shard.id = str(uuid4())
    pbshard.replicas.append(replica3)
    NODES["node3"] = Mock(
        sidecar=Mock(
            GetCount=AsyncMock(return_value=Counter(resources=1, paragraphs=1))
        )
    )

    sentry_sdk.capture_message.reset_mock()
    await shard.add_resource(resource, 1)
    assert sentry_sdk.capture_message.call_count == 1

    clean_utility(Utility.INDEXING)
    clean_utility(Utility.STORAGE)

    del NODES["node1"]
    del NODES["node2"]
    del NODES["node3"]
