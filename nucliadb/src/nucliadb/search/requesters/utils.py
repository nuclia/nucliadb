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
from typing import Any, Optional, Sequence, TypeVar, Union, overload

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

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.search import logger
from nucliadb.search.search.shards import (
    graph_search_shard,
    query_shard,
    suggest_shard,
)
from nucliadb.search.settings import settings
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from nucliadb_telemetry import errors


class Method(Enum):
    SEARCH = auto()
    SUGGEST = auto()
    GRAPH = auto()


METHODS = {
    Method.SEARCH: query_shard,
    Method.SUGGEST: suggest_shard,
    Method.GRAPH: graph_search_shard,
}

REQUEST_TYPE = Union[SuggestRequest, SearchRequest, GraphSearchRequest]

T = TypeVar(
    "T",
    SuggestResponse,
    SearchResponse,
    GraphSearchResponse,
)


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: SuggestRequest,
    timeout: Optional[float] = None,
) -> tuple[list[SuggestResponse], bool, list[str]]: ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: SearchRequest,
    timeout: Optional[float] = None,
) -> tuple[list[SearchResponse], bool, list[str]]: ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: GraphSearchRequest,
    timeout: Optional[float] = None,
) -> tuple[list[GraphSearchResponse], bool, list[str]]: ...


async def node_query(
    kbid: str,
    method: Method,
    pb_query: REQUEST_TYPE,
    timeout: Optional[float] = None,
) -> tuple[Sequence[Union[T, BaseException]], bool, list[str]]:
    timeout = timeout or settings.search_timeout
    shard_manager = get_shard_manager()
    try:
        shard_groups: list[PBShardObject] = await shard_manager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    ops = []
    queried_shards = []
    incomplete_results = False

    for shard_obj in shard_groups:
        shard_id = shard_obj.nidx_shard_id
        if shard_id is not None:
            # At least one node is alive for this shard group
            # let's add it ot the query list if has a valid value
            func = METHODS[method]
            ops.append(func(shard_id, pb_query))  # type: ignore
            queried_shards.append(shard_id)

    if not ops:
        logger.warning(f"No node found for any of this resources shards {kbid}")
        raise HTTPException(
            status_code=512,
            detail=f"No node found for any of this resources shards {kbid}",
        )

    try:
        results: list[Union[T, BaseException]] = await asyncio.wait_for(
            asyncio.gather(*ops, return_exceptions=True),
            timeout=timeout,
        )
    except asyncio.TimeoutError as exc:  # pragma: no cover
        logger.warning(
            "Timeout while querying nidx",
        )
        results = [exc]

    error = validate_node_query_results(results or [])
    if error is not None:
        query_dict = MessageToDict(pb_query)
        query_dict.pop("vector", None)
        logger.error(
            "Error while querying nodes",
            extra={
                "kbid": kbid,
                "query": json.dumps(query_dict),
            },
        )
        raise error

    return results, incomplete_results, queried_shards


def validate_node_query_results(results: list[Any]) -> Optional[HTTPException]:
    """
    Validate the results of a node query and return an exception if any error is found

    Handling of exception is responsibility of caller.
    """
    if results is None or len(results) == 0:
        return HTTPException(status_code=500, detail=f"Error while executing shard queries. No results.")

    for result in results:
        if isinstance(result, Exception):
            status_code = 500
            reason = "Error while querying shard data."
            if isinstance(result, AioRpcError):
                if result.code() is GrpcStatusCode.INTERNAL:
                    # handle node response errors
                    details = result.details() or "gRPC error without details"
                    if "AllButQueryForbidden" in details:
                        status_code = 412
                        reason = details.split(":")[-1].strip().strip("'")
                    else:
                        reason = details
                        logger.exception(f"Unhandled node error", exc_info=result)
                else:
                    logger.error(
                        f"Unhandled GRPC error while querying shard data: {result.debug_error_string()}"
                    )
            else:
                errors.capture_exception(result)
                logger.exception(f"Error while querying shard data {result}", exc_info=result)

            return HTTPException(status_code=status_code, detail=reason)

    return None
