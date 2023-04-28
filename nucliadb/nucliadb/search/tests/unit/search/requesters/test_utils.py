from unittest.mock import Mock

from fastapi import HTTPException
from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore

from nucliadb.search.requesters import utils


def test_validate_node_query_results():
    assert utils.validate_node_query_results([Mock()]) is None


def test_validate_node_query_results_no_results():
    assert isinstance(utils.validate_node_query_results([]), HTTPException)
    assert isinstance(utils.validate_node_query_results(None), HTTPException)


def test_validate_node_query_results_unhandled_error():
    error = utils.validate_node_query_results([Exception()])
    assert isinstance(error, HTTPException)


def test_validate_node_query_results_unavailable():
    result = utils.validate_node_query_results(
        [
            AioRpcError(
                code=StatusCode.UNAVAILABLE,
                initial_metadata=Mock(),
                trailing_metadata=Mock(),
                details="",
                debug_error_string="",
            )
        ]
    )

    assert isinstance(result, HTTPException)
    assert result.status_code == 503


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
