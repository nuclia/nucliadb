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
#
from fastapi import HTTPException, Request
from fastapi_versioning import version

from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.counters import IndexCounts
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb_models.internal.shards import KnowledgeboxShards
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    KnowledgeboxCounters,
    SearchParamDefaults,
)
from nucliadb_utils.authentication import requires, requires_one

MAX_PARAGRAPHS_FOR_SMALL_KB = 250_000


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/shards",
    status_code=200,
    description="Show shards from a knowledgebox",
    response_model=KnowledgeboxShards,
    include_in_schema=False,
    tags=["Knowledge Boxes"],
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def knowledgebox_shards(request: Request, kbid: str) -> KnowledgeboxShards:
    return KnowledgeboxShards(kbid=kbid, shards=[])


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/counters",
    status_code=200,
    description="Summary of amount of different things inside a knowledgebox",
    response_model=KnowledgeboxCounters,
    tags=["Knowledge Boxes"],
    response_model_exclude_unset=True,
)
@requires_one([NucliaDBRoles.READER, NucliaDBRoles.MANAGER])
@version(1)
async def knowledgebox_counters(
    request: Request,
    kbid: str,
    debug: bool = fastapi_query(SearchParamDefaults.debug),
) -> KnowledgeboxCounters:
    try:
        return await _kb_counters(kbid, debug=debug)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )


async def _kb_counters(
    kbid: str,
    debug: bool = False,
) -> KnowledgeboxCounters:
    """
    Resources count is calculated from maindb and cached
    Field count is calculated from the index node cluster
    Paragraphs and Sentences count is calculated from the index node cluster or the external index provider.
    Index size is estimated from the paragraphs count.
    """
    counters = KnowledgeboxCounters(
        resources=await datamanagers.atomic.resources.count(kbid=kbid),
        paragraphs=0,
        fields=0,
        sentences=0,
        index_size=0,
    )
    node_index_counts, queried_shards = await get_node_index_counts(kbid)
    counters.fields = node_index_counts.fields
    counters.paragraphs = node_index_counts.paragraphs
    counters.sentences = node_index_counts.sentences
    counters.index_size = node_index_counts.size_bytes
    if debug and queried_shards is not None:
        counters.shards = queried_shards
    return counters


async def get_node_index_counts(kbid: str) -> tuple[IndexCounts, list[str]]:
    # TODO(Marklogic): Read index counts scoped to the knowledgebox.
    return IndexCounts(fields=0, paragraphs=0, sentences=0, size_bytes=0), []
