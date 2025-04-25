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

from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException
from grpc import StatusCode
from grpc.aio import AioRpcError

from nucliadb.search.requesters import utils
from nucliadb_protos import writer_pb2
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


@pytest.fixture
def shard_manager():
    original = get_utility(Utility.SHARD_MANAGER)

    manager = AsyncMock()
    manager.get_shards_by_kbid = AsyncMock(
        return_value=[
            writer_pb2.ShardObject(
                shard="shard-id",
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
def mocked_search_methods():
    methods = {utils.Method.SEARCH: AsyncMock()}
    with patch.dict(utils.METHODS, methods, clear=True):
        yield methods


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
