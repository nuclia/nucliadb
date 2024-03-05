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
from time import time

from nucliadb.search.requesters.utils import Method, debug_nodes_info, node_query
from nucliadb.search.search.find_merge import find_merge_results
from nucliadb.search.search.query import QueryParser
from nucliadb.search.search.utils import (
    min_score_from_payload,
    should_disable_vector_search,
)
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    SearchOptions,
)
from nucliadb_utils.utilities import get_audit


async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> tuple[KnowledgeboxFindResults, bool]:
    audit = get_audit()
    start_time = time()

    item.min_score = min_score_from_payload(item.min_score)

    if SearchOptions.VECTOR in item.features:
        if should_disable_vector_search(item):
            item.features.remove(SearchOptions.VECTOR)

    query_parser = QueryParser(
        kbid=kbid,
        features=item.features,
        query=item.query,
        filters=item.filters,
        faceted=None,
        sort=None,
        page_number=item.page_number,
        page_size=item.page_size,
        min_score=item.min_score,
        range_creation_start=item.range_creation_start,
        range_creation_end=item.range_creation_end,
        range_modification_start=item.range_modification_start,
        range_modification_end=item.range_modification_end,
        fields=item.fields,
        user_vector=item.vector,
        vectorset=item.vectorset,
        with_duplicates=item.with_duplicates,
        with_synonyms=item.with_synonyms,
        autofilter=item.autofilter,
        key_filters=item.resource_filters,
        security=item.security,
    )
    pb_query, incomplete_results, autofilters = await query_parser.parse()
    results, query_incomplete_results, queried_nodes = await node_query(
        kbid, Method.SEARCH, pb_query, target_shard_replicas=item.shards
    )
    incomplete_results = incomplete_results or query_incomplete_results

    # We need to merge
    search_results = await find_merge_results(
        results,
        count=item.page_size,
        page=item.page_number,
        kbid=kbid,
        show=item.show,
        field_type_filter=item.field_type_filter,
        extracted=item.extracted,
        requested_relations=pb_query.relation_subgraph,
        min_score_bm25=query_parser.min_score.bm25,
        min_score_semantic=query_parser.min_score.semantic,
        highlight=item.highlight,
    )

    if audit is not None:
        await audit.search(
            kbid,
            x_nucliadb_user,
            x_ndb_client.to_proto(),
            x_forwarded_for,
            pb_query,
            time() - start_time,
            len(search_results.resources),
        )
    if item.debug:
        search_results.nodes = debug_nodes_info(queried_nodes)

    queried_shards = [shard_id for _, shard_id in queried_nodes]
    search_results.shards = queried_shards
    search_results.autofilters = autofilters
    return search_results, incomplete_results
