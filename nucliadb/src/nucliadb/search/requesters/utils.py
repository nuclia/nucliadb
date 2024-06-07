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
from enum import Enum
from typing import Any, Optional, Sequence, TypeVar, Union, overload

from fastapi import HTTPException
from google.protobuf.json_format import MessageToDict
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError

from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.search import logger
from nucliadb.search.search.shards import (
    query_paragraph_shard,
    query_shard,
    relations_shard,
    suggest_shard,
)
from nucliadb.search.settings import settings
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
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature


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

REQUEST_TYPE = Union[SuggestRequest, ParagraphSearchRequest, SearchRequest, RelationSearchRequest]

T = TypeVar(
    "T",
    SuggestResponse,
    ParagraphSearchResponse,
    SearchResponse,
    RelationSearchResponse,
)


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: SuggestRequest,
    target_shard_replicas: Optional[list[str]] = None,
    use_read_replica_nodes: bool = True,
) -> tuple[list[SuggestResponse], bool, list[tuple[AbstractIndexNode, str]]]: ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: ParagraphSearchRequest,
    target_shard_replicas: Optional[list[str]] = None,
    use_read_replica_nodes: bool = True,
) -> tuple[list[ParagraphSearchResponse], bool, list[tuple[AbstractIndexNode, str]]]: ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: SearchRequest,
    target_shard_replicas: Optional[list[str]] = None,
    use_read_replica_nodes: bool = True,
) -> tuple[list[SearchResponse], bool, list[tuple[AbstractIndexNode, str]]]: ...


@overload
async def node_query(
    kbid: str,
    method: Method,
    pb_query: RelationSearchRequest,
    target_shard_replicas: Optional[list[str]] = None,
    use_read_replica_nodes: bool = True,
) -> tuple[list[RelationSearchResponse], bool, list[tuple[AbstractIndexNode, str]]]: ...


async def node_query(
    kbid: str,
    method: Method,
    pb_query: REQUEST_TYPE,
    target_shard_replicas: Optional[list[str]] = None,
    use_read_replica_nodes: bool = True,
) -> tuple[Sequence[Union[T, BaseException]], bool, list[tuple[AbstractIndexNode, str]]]:
    use_read_replica_nodes = use_read_replica_nodes and has_feature(
        const.Features.READ_REPLICA_SEARCHES, context={"kbid": kbid}
    )

    shard_manager = get_shard_manager()
    try:
        shard_groups: list[PBShardObject] = await shard_manager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    ops = []
    queried_nodes = []
    incomplete_results = False

    for shard_obj in shard_groups:
        try:
            node, shard_id = cluster_manager.choose_node(
                shard_obj,
                use_read_replica_nodes=use_read_replica_nodes,
                target_shard_replicas=target_shard_replicas,
            )
        except KeyError:
            incomplete_results = True
        else:
            if shard_id is not None:
                # At least one node is alive for this shard group
                # let's add it ot the query list if has a valid value
                func = METHODS[method]
                ops.append(func(node, shard_id, pb_query))  # type: ignore
                queried_nodes.append((node, shard_id))

    if not ops:
        logger.warning(f"No node found for any of this resources shards {kbid}")
        raise HTTPException(
            status_code=512,
            detail=f"No node found for any of this resources shards {kbid}",
        )

    try:
        results: list[Union[T, BaseException]] = await asyncio.wait_for(
            asyncio.gather(*ops, return_exceptions=True),
            timeout=settings.search_timeout,
        )
    except asyncio.TimeoutError as exc:  # pragma: no cover
        logger.warning(
            "Timeout while querying nodes",
            extra={"nodes": debug_nodes_info(queried_nodes)},
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
        if (
            error.status_code >= 500
            and use_read_replica_nodes
            and any([node.is_read_replica() for node, _ in queried_nodes])
        ):
            # We had an error querying a secondary node, instead of raising an
            # error directly, retry query to primaries and hope it works
            logger.warning(
                "Query to read replica failed. Trying again with primary",
                extra={"nodes": debug_nodes_info(queried_nodes)},
            )

            results, incomplete_results, primary_queried_nodes = await node_query(  # type: ignore
                kbid,
                method,
                pb_query,
                target_shard_replicas,
                use_read_replica_nodes=False,
            )
            queried_nodes.extend(primary_queried_nodes)
            return results, incomplete_results, queried_nodes

        raise error

    return results, incomplete_results, queried_nodes


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
                logger.exception("Error while querying shard data", exc_info=result)

            return HTTPException(status_code=status_code, detail=reason)

    return None


def debug_nodes_info(nodes: list[tuple[AbstractIndexNode, str]]) -> list[dict[str, str]]:
    details: list[dict[str, str]] = []
    for node, shard_id in nodes:
        info = {
            "id": node.id,
            "shard_id": shard_id,
            "address": node.address,
        }
        if node.primary_id:
            info["primary_id"] = node.primary_id
        details.append(info)
    return details
