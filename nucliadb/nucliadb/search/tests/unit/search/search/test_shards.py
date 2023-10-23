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
import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from nucliadb_protos.nodereader_pb2 import SearchRequest

from nucliadb.search.search.shards import node_observer, query_shard


async def test_node_observer_records_timeout_errors():
    node = Mock(id="node-1")
    # When waiting for a task to finish with asyncio, if it times out asyncio will
    # cancell the task throwing a CancelledError on that task
    node.reader.Search = AsyncMock(side_effect=asyncio.CancelledError)
    query = SearchRequest(body="foo")

    node_observer.counter.clear()

    with pytest.raises(asyncio.CancelledError):
        await query_shard(node, "shard", query)

    sample = node_observer.counter.collect()[0].samples[0]
    assert sample.name == "node_client_count_total"
    assert sample.labels["type"] == "search"
    assert sample.labels["node_id"] == "node-1"
    assert sample.labels["status"] == "timeout"
