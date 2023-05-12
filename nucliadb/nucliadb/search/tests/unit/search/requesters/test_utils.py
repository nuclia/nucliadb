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

from unittest.mock import MagicMock, Mock

import pytest
from fastapi import HTTPException
from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore

from nucliadb.search.requesters import utils


def test_validate_node_query_results():
    assert utils.validate_node_query_results([Mock()], []) is None


def test_validate_node_query_results_no_results():
    assert isinstance(utils.validate_node_query_results([], []), HTTPException)
    assert isinstance(utils.validate_node_query_results(None, []), HTTPException)


def test_validate_node_query_results_unhandled_error():
    error = utils.validate_node_query_results([Exception()], [])
    assert isinstance(error, HTTPException)


def test_validate_node_query_results_unavailable_reset_conns():
    # if result len match used nodes len, just reset the connection
    # from the used node
    node = MagicMock()
    with pytest.raises(utils.RetriableNodeQueryException):
        utils.validate_node_query_results(
            [
                AioRpcError(
                    code=StatusCode.UNAVAILABLE,
                    initial_metadata=Mock(),
                    trailing_metadata=Mock(),
                    details="",
                    debug_error_string="",
                )
            ],
            [node],
        )

    node.reset_connections.assert_called_once()


def test_validate_node_query_results_unavailable_reset_all_node_conns():
    node1 = MagicMock()
    node2 = MagicMock()
    with pytest.raises(utils.RetriableNodeQueryException):
        utils.validate_node_query_results(
            [
                AioRpcError(
                    code=StatusCode.UNAVAILABLE,
                    initial_metadata=Mock(),
                    trailing_metadata=Mock(),
                    details="",
                    debug_error_string="",
                )
            ],
            [node1, node2],
        )

    node1.reset_connections.assert_called_once()
    node2.reset_connections.assert_called_once()


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
        ],
        [],
    )

    assert isinstance(result, HTTPException)
    assert result.status_code == 412
    assert result.detail == "Query is invalid. AllButQueryForbidden"
