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
from typing import assert_never, overload

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
from nucliadb.search.search.backend import graph_search, hybrid_search, suggest
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
    timeout = timeout or settings.search_timeout
    result: SearchResponse | SuggestResponse | GraphSearchResponse
    try:
        if method == Method.SEARCH:
            if not isinstance(pb_query, SearchRequest):
                raise TypeError("SEARCH requires a SearchRequest")
            result = await asyncio.wait_for(hybrid_search(kbid, pb_query), timeout=timeout)
        elif method == Method.SUGGEST:
            if not isinstance(pb_query, SuggestRequest):
                raise TypeError("SUGGEST requires a SuggestRequest")
            result = await asyncio.wait_for(suggest(kbid, pb_query), timeout=timeout)
        elif method == Method.GRAPH:
            if not isinstance(pb_query, GraphSearchRequest):
                raise TypeError("GRAPH requires a GraphSearchRequest")
            result = await asyncio.wait_for(graph_search(kbid, pb_query), timeout=timeout)
        else:  # pragma: no cover
            assert_never(method)
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
    reason = "Error while querying search backend."
    if isinstance(exc, asyncio.TimeoutError):
        logger.warning("Timeout while querying index", extra=extra)
    else:
        errors.capture_exception(exc)
        logger.exception(f"Error while querying index: {exc}", extra=extra, exc_info=exc)

    return HTTPException(status_code=status_code, detail=reason)
