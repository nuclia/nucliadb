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

import pytest
from fastapi import HTTPException
from grpc import StatusCode
from grpc.aio import AioRpcError

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.search.requesters import utils
from nucliadb_protos import nodereader_pb2, writer_pb2
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


@pytest.fixture
def fake_nodes():
    from nucliadb.common.cluster import manager

    original = manager.INDEX_NODES
    manager.INDEX_NODES.clear()

    manager.add_index_node(
        id="node-0",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
    )
    manager.add_index_node(
        id="node-replica-0",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
        primary_id="node-0",
    )

    yield (["node-0"], ["node-replica-0"])

    manager.INDEX_NODES = original


@pytest.fixture
def shard_manager():
    original = get_utility(Utility.SHARD_MANAGER)

    manager = AsyncMock()
    manager.get_shards_by_kbid = AsyncMock(
        return_value=[
            writer_pb2.ShardObject(
                shard="shard-id",
                replicas=[
                    writer_pb2.ShardReplica(
                        shard=writer_pb2.ShardCreated(id="shard-id"), node="node-0"
                    )
                ],
            )
        ]
    )

    set_utility(Utility.SHARD_MANAGER, manager)

    yield manager

    if original is None:
        clean_utility(Utility.SHARD_MANAGER)
    else:
        set_utility(Utility.SHARD_MANAGER, original)


@pytest.fixture()
def search_methods():
    def fake_search(
        node: AbstractIndexNode, shard: str, query: nodereader_pb2.SearchRequest
    ):
        if node.is_read_replica():
            raise Exception()
        return nodereader_pb2.SearchResponse()

    original = utils.METHODS
    utils.METHODS = {
        utils.Method.SEARCH: AsyncMock(side_effect=fake_search),
        utils.Method.PARAGRAPH: AsyncMock(),
    }

    yield utils.METHODS

    utils.METHODS = original


@pytest.mark.asyncio
async def test_node_query_retries_primary_if_secondary_fails(
    fake_nodes,
    shard_manager,
    search_methods,
):
    """Setting up a node and a faulty replica, validate primary is queried if
    secondary fails.

    """
    results, incomplete_results, queried_nodes = await utils.node_query(
        kbid="my-kbid",
        method=utils.Method.SEARCH,
        pb_query=Mock(),
        use_read_replica_nodes=True,
    )
    # secondary fails, primary is called
    assert search_methods[utils.Method.SEARCH].await_count == 2
    assert len(queried_nodes) == 2
    assert queried_nodes[0][0].is_read_replica()
    assert not queried_nodes[1][0].is_read_replica()

    results, incomplete_results, queried_nodes = await utils.node_query(
        kbid="my-kbid",
        method=utils.Method.PARAGRAPH,
        pb_query=Mock(),
        use_read_replica_nodes=True,
    )
    # secondary succeeds, no fallback call to primary
    assert search_methods[utils.Method.PARAGRAPH].await_count == 1
    assert len(queried_nodes) == 1
    assert queried_nodes[0][0].is_read_replica()


def test_debug_nodes_info(fake_nodes: tuple[list[str], list[str]]):
    from nucliadb.common.cluster import manager

    primary = manager.get_index_node(fake_nodes[0][0])
    assert primary is not None
    secondary = manager.get_index_node(fake_nodes[1][0])
    assert secondary is not None

    info = utils.debug_nodes_info([(primary, "shard-a"), (secondary, "shard-b")])
    assert len(info) == 2

    primary_keys = ["id", "shard_id", "address"]
    secondary_keys = primary_keys + ["primary_id"]

    for key in primary_keys:
        assert key in info[0]

    for key in secondary_keys:
        assert key in info[1]


def test_validate_node_query_results():
    assert utils.validate_node_query_results([Mock()]) is None


def test_validate_node_query_results_no_results():
    assert isinstance(utils.validate_node_query_results([]), HTTPException)
    assert isinstance(utils.validate_node_query_results(None), HTTPException)


def test_validate_node_query_results_unhandled_error():
    error = utils.validate_node_query_results([Exception()])
    assert isinstance(error, HTTPException)


def test_validate_node_query_results_invalid_query():
    result = utils.validate_node_query_results(
        [
            AioRpcError(
                code=StatusCode.INTERNAL,
                initial_metadata=Mock(),
                trailing_metadata=Mock(),
                details="An invalid argument was passed: 'Query is invalid. AllButQueryForbidden'",
                debug_error_string="",
            )
        ]
    )

    assert isinstance(result, HTTPException)
    assert result.status_code == 412
    assert result.detail == "Query is invalid. AllButQueryForbidden"


def test_validate_node_query_results_internal_unhandled():
    result = utils.validate_node_query_results(
        [
            AioRpcError(
                code=StatusCode.INTERNAL,
                initial_metadata=Mock(),
                trailing_metadata=Mock(),
                details="There is something wrong with your query, my friend!",
                debug_error_string="This query is simply wrong",
            )
        ]
    )
    assert isinstance(result, HTTPException)
    assert result.status_code == 500
    assert result.detail == "There is something wrong with your query, my friend!"
