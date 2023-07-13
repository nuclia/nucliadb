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
from enum import Enum
from typing import Any, List, Optional, Tuple, TypeVar, Union, overload

from fastapi import HTTPException
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.nodereader_pb2 import (
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    RelationSearchRequest,
    RelationSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.manager import choose_node
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.ingest.txn_utils import abort_transaction
from nucliadb.search import logger
from nucliadb.search.search.shards import (
    query_paragraph_shard,
    query_shard,
    relations_shard,
    suggest_shard,
)
from nucliadb.search.settings import settings
from nucliadb_telemetry import errors


class Method(Enum):
    SEARCH = 1
    PARAGRAPH = 2
    SUGGEST = 3
    RELATIONS = 4


METHODS = {
    Method.SEARCH: query_shard,
    Method.PARAGRAPH: query_paragraph_shard,
    Method.SUGGEST: suggest_shard,
    Method.RELATIONS: relations_shard,
}

REQUEST_TYPE = Union[
    SuggestRequest, ParagraphSearchRequest, SearchRequest, RelationSearchRequest
]

T = TypeVar(
    "T",
    SuggestResponse,
    ParagraphSearchResponse,
    SearchResponse,
    RelationSearchResponse,
)


@overload  # type: ignore
async def node_query(
    kbid: str,
    method: Method,
    pb_query: SuggestRequest,
    shards: Optional[List[str]] = None,
) -> Tuple[List[SuggestResponse], bool, List[Tuple[str, str, str]], List[str]]:
    ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: ParagraphSearchRequest,
    shards: Optional[List[str]] = None,
) -> Tuple[List[ParagraphSearchResponse], bool, List[Tuple[str, str, str]], List[str]]:
    ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: SearchRequest,
    shards: Optional[List[str]] = None,
) -> Tuple[List[SearchResponse], bool, List[Tuple[str, str, str]], List[str]]:
    ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: RelationSearchRequest,
    shards: Optional[List[str]] = None,
) -> Tuple[List[RelationSearchResponse], bool, List[Tuple[str, str, str]], List[str]]:
    ...


async def node_query(
    kbid: str,
    method: Method,
    pb_query: REQUEST_TYPE,
    shards: Optional[List[str]] = None,
) -> Tuple[List[T], bool, List[Tuple[str, str, str]], List[str]]:
    shard_manager = get_shard_manager()

    try:
        shard_groups: List[PBShardObject] = await shard_manager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    ops = []
    queried_shards = []
    queried_nodes = []
    incomplete_results = False

    for shard_obj in shard_groups:
        try:
            node, shard_id, node_id = choose_node(shard_obj, target_replicas=shards)
        except KeyError:
            incomplete_results = True
        else:
            if shard_id is not None:
                # At least one node is alive for this shard group
                # let's add it ot the query list if has a valid value
                func = METHODS[method]
                ops.append(func(node, shard_id, pb_query))  # type: ignore
                queried_nodes.append((node.label, shard_id, node_id))
                queried_shards.append(shard_id)

    if not ops:
        await abort_transaction()
        logger.warning(f"No node found for any of this resources shards {kbid}")
        raise HTTPException(
            status_code=512,
            detail=f"No node found for any of this resources shards {kbid}",
        )

    try:
        results = await asyncio.wait_for(  # type: ignore
            asyncio.gather(*ops, return_exceptions=True),  # type: ignore
            timeout=settings.search_timeout,
        )
    except asyncio.TimeoutError as exc:
        results = [exc]

    error = validate_node_query_results(results or [])
    if error is not None:
        await abort_transaction()
        raise error

    return results, incomplete_results, queried_nodes, queried_shards


def validate_node_query_results(results: list[Any]) -> Optional[HTTPException]:
    """
    Validate the results of a node query and return an exception if any error is found

    Handling of exception is responsibility of caller.
    """
    if results is None or len(results) == 0:
        return HTTPException(
            status_code=500, detail=f"Error while executing shard queries. No results."
        )

    for result in results:
        if isinstance(result, Exception):
            status_code = 500
            reason = "Error while querying shard data."
            if isinstance(result, AioRpcError):
                if result.code() is GrpcStatusCode.INTERNAL:
                    # handle node response errors
                    if "AllButQueryForbidden" in result.details():
                        status_code = 412
                        reason = result.details().split(":")[-1].strip().strip("'")
                    else:
                        reason = result.details()
                        logger.exception(f"Unhandled node error", exc_info=result)
                else:
                    logger.error(
                        f"Unhandled GRPC error while querying shard data: {result.debug_error_string()}"
                    )
            else:
                errors.capture_exception(result)
                logger.exception("Error while querying shard data", exc_info=result)

            return HTTPException(status_code=status_code, detail=reason)

    return None
