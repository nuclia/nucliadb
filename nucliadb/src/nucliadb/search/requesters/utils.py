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

import asyncio
import json
from enum import Enum, auto
from typing import Awaitable, Callable, assert_never, overload

from fastapi import HTTPException
from google.protobuf.json_format import MessageToDict
from nidx_protos.nodereader_pb2 import (
    GraphSearchRequest,
    GraphSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)

from nucliadb.search import logger
from nucliadb.search.search.shards import graph_search_shards, query_shards, suggest_shards
from nucliadb.search.settings import settings
from nucliadb_telemetry import errors


class Method(Enum):
    SEARCH = auto()
    SUGGEST = auto()
    GRAPH = auto()


@overload
async def query_index(
    kbid: str,
    method: Method,
    pb_query: SuggestRequest,
    timeout: float | None = None,
) -> SuggestResponse: ...


@overload
async def query_index(
    kbid: str,
    method: Method,
    pb_query: SearchRequest,
    timeout: float | None = None,
) -> SearchResponse: ...


@overload
async def query_index(
    kbid: str,
    method: Method,
    pb_query: GraphSearchRequest,
    timeout: float | None = None,
) -> GraphSearchResponse: ...


async def query_index(
    kbid: str,
    method: Method,
    pb_query: SearchRequest | SuggestRequest | GraphSearchRequest,
    timeout: float | None = None,
) -> SearchResponse | SuggestResponse | GraphSearchResponse:
    # TODO(Marklogic): Route requests by knowledgebox instead of physical index shards.
    shards: list[str] = []

    func: (
        Callable[[list[str], SearchRequest], Awaitable[SearchResponse]]
        | Callable[[list[str], SuggestRequest], Awaitable[SuggestResponse]]
        | Callable[[list[str], GraphSearchRequest], Awaitable[GraphSearchResponse]]
    )
    if method == Method.SEARCH:
        func = query_shards
    elif method == Method.SUGGEST:
        func = suggest_shards
    elif method == Method.GRAPH:
        func = graph_search_shards
    else:  # pragma: no cover
        assert_never(method)

    timeout = timeout or settings.search_timeout
    result: SearchResponse | SuggestResponse | GraphSearchResponse
    try:
        result = await asyncio.wait_for(
            func(
                shards,
                pb_query,  # type: ignore[arg-type,ty:invalid-argument-type]
            ),
            timeout=timeout,
        )
    except Exception as exc:
        query_dict = MessageToDict(pb_query)
        query_dict.pop("vector", None)
        extra = {
            "kbid": kbid,
            "query": json.dumps(query_dict),
        }
        raise handle_index_exception(exc, extra=extra)

    return result


def handle_index_exception(exc: Exception, *, extra: dict[str, str]) -> HTTPException:
    """Parse a backend exception, log necessary details and return an equivalent
    HTTP exception to raise to the user."""

    status_code = 500
    reason = "Error while querying shard data."
    if isinstance(exc, asyncio.TimeoutError):
        logger.warning("Timeout while querying index", extra=extra)
    else:
        errors.capture_exception(exc)
        logger.exception(f"Error while querying index: {exc}", extra=extra, exc_info=exc)

    return HTTPException(status_code=status_code, detail=reason)
