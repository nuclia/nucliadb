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
from typing import Awaitable, Callable, overload

from fastapi import HTTPException
from google.protobuf.json_format import MessageToDict
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError
from nidx_protos.nodereader_pb2 import (
    GraphSearchRequest,
    GraphSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)
from typing_extensions import assert_never

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.search import logger
from nucliadb.search.search.shards import graph_search_shards, query_shards, suggest_shards
from nucliadb.search.settings import settings
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from nucliadb_telemetry import errors


class Method(Enum):
    SEARCH = auto()
    SUGGEST = auto()
    GRAPH = auto()


@overload
async def nidx_query(
    kbid: str,
    method: Method,
    pb_query: SuggestRequest,
    timeout: float | None = None,
) -> tuple[SuggestResponse, list[str]]: ...


@overload
async def nidx_query(
    kbid: str,
    method: Method,
    pb_query: SearchRequest,
    timeout: float | None = None,
) -> tuple[SearchResponse, list[str]]: ...


@overload
async def nidx_query(
    kbid: str,
    method: Method,
    pb_query: GraphSearchRequest,
    timeout: float | None = None,
) -> tuple[GraphSearchResponse, list[str]]: ...


async def nidx_query(
    kbid: str,
    method: Method,
    pb_query: SearchRequest | SuggestRequest | GraphSearchRequest,
    timeout: float | None = None,
) -> tuple[SearchResponse | SuggestResponse | GraphSearchResponse | BaseException, list[str]]:
    shard_manager = get_shard_manager()
    try:
        shard_groups: list[PBShardObject] = await shard_manager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    queried_shards = []
    for shard_obj in shard_groups:
        if shard_obj.nidx_shard_id is not None:
            queried_shards.append(shard_obj.nidx_shard_id)

    if len(queried_shards) == 0:
        logger.warning(f"No shards found for kb", extra={"kbid": kbid})
        raise HTTPException(
            status_code=512,
            detail=f"No shards found for kb",
        )

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
                queried_shards,
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
        raise handle_nidx_exception(exc, extra=extra)

    return result, queried_shards


def handle_nidx_exception(exc: Exception, *, extra: dict[str, str]) -> HTTPException:
    """Parse a nidx exception, log necessary details and return an equivalent
    HTTP exception to raise to the user."""

    status_code = 500
    reason = "Error while querying shard data."
    if isinstance(exc, AioRpcError):
        if exc.code() is GrpcStatusCode.INTERNAL:
            # handle nidx response errors
            details = exc.details() or "gRPC error without details"
            if "AllButQueryForbidden" in details:
                status_code = 412
                reason = details.split(":")[-1].strip().strip("'")
            else:
                reason = details
                logger.exception(f"Unhandled nidx error", extra=extra, exc_info=exc)
        else:
            logger.error(
                f"Unhandled GRPC error while querying nidx: {exc.debug_error_string()}", extra=extra
            )

    elif isinstance(exc, asyncio.TimeoutError):
        logger.warning("Timeout while querying nidx", extra=extra)
    else:
        errors.capture_exception(exc)
        logger.exception(f"Error while querying nidx: {exc}", extra=extra, exc_info=exc)

    return HTTPException(status_code=status_code, detail=reason)
