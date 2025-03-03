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
import asyncio
import string
from datetime import datetime
from typing import Any, Awaitable, Optional

from nucliadb.common import datamanagers
from nucliadb.common.models_utils.from_proto import RelationNodeTypeMap
from nucliadb.search import logger
from nucliadb.search.predict import SendToPredictError
from nucliadb.search.search.filters import (
    translate_label,
)
from nucliadb.search.search.metrics import (
    node_features,
)
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.rank_fusion import (
    RankFusionAlgorithm,
)
from nucliadb.search.search.rerankers import (
    Reranker,
)
from nucliadb_models.filters import FilterExpression
from nucliadb_models.internal.predict import QueryInfo
from nucliadb_models.labels import LABEL_HIDDEN, translate_system_to_alias_label
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    KnowledgeGraphEntity,
    MaxTokens,
    MinScore,
    SearchOptions,
    SortField,
    SortOptions,
    SortOrder,
    SortOrderMap,
    SuggestOptions,
)
from nucliadb_models.security import RequestSecurity
from nucliadb_protos import nodereader_pb2, utils_pb2
from nucliadb_protos.noderesources_pb2 import Resource

from .exceptions import InvalidQueryError
from .query_parser.filter_expression import add_and_expression, parse_expression
from .query_parser.old_filters import OldFilterParams, parse_old_filters

INDEX_SORTABLE_FIELDS = [
    SortField.CREATED,
    SortField.MODIFIED,
]

DEFAULT_GENERIC_SEMANTIC_THRESHOLD = 0.7


class QueryParser:
    """
    Queries are getting more and more complex and different phases of the query
    depending on different data.

    This class is an encapsulation of the different phases of the query and allow
    some stateful interaction with a query and different depenedencies during
    query parsing.
    """

    _query_information_task: Optional[asyncio.Task] = None

    def __init__(
        self,
        *,
        kbid: str,
        features: list[SearchOptions],
        query: str,
        top_k: int,
        min_score: MinScore,
        old_filters: OldFilterParams,
        filter_expression: Optional[FilterExpression] = None,
        query_entities: Optional[list[KnowledgeGraphEntity]] = None,
        faceted: Optional[list[str]] = None,
        sort: Optional[SortOptions] = None,
        user_vector: Optional[list[float]] = None,
        vectorset: Optional[str] = None,
        with_duplicates: bool = False,
        with_status: Optional[ResourceProcessingStatus] = None,
        with_synonyms: bool = False,
        autofilter: bool = False,
        security: Optional[RequestSecurity] = None,
        generative_model: Optional[str] = None,
        rephrase: bool = False,
        rephrase_prompt: Optional[str] = None,
        max_tokens: Optional[MaxTokens] = None,
        hidden: Optional[bool] = None,
        rank_fusion: Optional[RankFusionAlgorithm] = None,
        reranker: Optional[Reranker] = None,
    ):
        self.kbid = kbid
        self.features = features
        self.query = query
        self.query_entities = query_entities
        self.hidden = hidden
        self.faceted = faceted or []
        self.top_k = top_k
        self.min_score = min_score
        self.sort = sort
        self.user_vector = user_vector
        self.vectorset = vectorset
        self.with_duplicates = with_duplicates
        self.with_status = with_status
        self.with_synonyms = with_synonyms
        self.autofilter = autofilter
        self.security = security
        self.generative_model = generative_model
        self.rephrase = rephrase
        self.rephrase_prompt = rephrase_prompt
        self.query_endpoint_used = False
        self.max_tokens = max_tokens
        self.rank_fusion = rank_fusion
        self.reranker = reranker
        self.filter_expression = filter_expression
        self.old_filters = old_filters
        self.fetcher = Fetcher(
            kbid=kbid,
            query=query,
            user_vector=user_vector,
            vectorset=vectorset,
            rephrase=rephrase,
            rephrase_prompt=rephrase_prompt,
            generative_model=generative_model,
        )

    @property
    def has_vector_search(self) -> bool:
        return SearchOptions.SEMANTIC in self.features

    @property
    def has_relations_search(self) -> bool:
        return SearchOptions.RELATIONS in self.features

    def _get_query_information(self) -> Awaitable[QueryInfo]:
        if self._query_information_task is None:  # pragma: no cover
            self._query_information_task = asyncio.create_task(self._query_information())
        return self._query_information_task

    async def _query_information(self) -> QueryInfo:
        # HACK: while transitioning to the new query parser, use fetcher under
        # the hood for a smoother migration
        query_info = await self.fetcher._predict_query_endpoint()
        if query_info is None:
            raise SendToPredictError("Error while using predict's query endpoint")
        return query_info

    async def _schedule_dependency_tasks(self) -> None:
        """
        This will schedule concurrent tasks for different data that needs to be pulled
        for the sake of the query being performed
        """
        if len(self.old_filters.label_filters) > 0:
            asyncio.ensure_future(self.fetcher.get_classification_labels())

        if self.has_vector_search and self.user_vector is None:
            self.query_endpoint_used = True
            asyncio.ensure_future(self._get_query_information())
            # XXX: should we also ensure get_vectorset and get_query_vector?
            asyncio.ensure_future(self.fetcher.get_matryoshka_dimension())

        if (self.has_relations_search or self.autofilter) and len(self.query) > 0:
            if not self.query_endpoint_used:
                # If we only need to detect entities, we don't need the query endpoint
                asyncio.ensure_future(self.fetcher.get_detected_entities())
            asyncio.ensure_future(self.fetcher.get_entities_meta_cache())
            asyncio.ensure_future(self.fetcher.get_deleted_entity_groups())
        if self.with_synonyms and self.query:
            asyncio.ensure_future(self.fetcher.get_synonyms())

    async def parse(self) -> tuple[nodereader_pb2.SearchRequest, bool, list[str], Optional[str]]:
        """
        :return: (request, incomplete, autofilters)
            where:
                - request: protobuf nodereader_pb2.SearchRequest object
                - incomplete: If the query is incomplete (missing vectors)
                - autofilters: The autofilters that were applied
        """
        request = nodereader_pb2.SearchRequest()
        request.body = self.query
        request.with_duplicates = self.with_duplicates

        self.parse_sorting(request)

        await self._schedule_dependency_tasks()

        await self.parse_filters(request)
        self.parse_document_search(request)
        self.parse_paragraph_search(request)
        incomplete, rephrased_query = await self.parse_vector_search(request)
        autofilters = await self.parse_relation_search(request)
        await self.parse_synonyms(request)
        await self.parse_min_score(request, incomplete)
        await self.adjust_page_size(request, self.rank_fusion, self.reranker)
        return request, incomplete, autofilters, rephrased_query

    async def parse_filters(self, request: nodereader_pb2.SearchRequest) -> None:
        request.faceted.labels.extend([translate_label(facet) for facet in self.faceted])

        if self.security is not None and len(self.security.groups) > 0:
            security_pb = utils_pb2.Security()
            for group_id in self.security.groups:
                if group_id not in security_pb.access_groups:
                    security_pb.access_groups.append(group_id)
            request.security.CopyFrom(security_pb)

        has_old_filters = False
        if self.old_filters:
            field_expr, paragraph_expr = await parse_old_filters(self.old_filters, self.fetcher)
            if field_expr is not None:
                request.field_filter.CopyFrom(field_expr)
                has_old_filters = True
            if paragraph_expr is not None:
                request.paragraph_filter.CopyFrom(paragraph_expr)
                has_old_filters = True

        if self.filter_expression and has_old_filters:
            raise InvalidQueryError("filter_expression", "Cannot mix old filters with filter_expression")

        if self.filter_expression:
            if self.filter_expression.field:
                expr = await parse_expression(self.filter_expression.field, self.kbid)
                if expr:
                    request.field_filter.CopyFrom(expr)

            if self.filter_expression.paragraph:
                expr = await parse_expression(self.filter_expression.paragraph, self.kbid)
                if expr:
                    request.paragraph_filter.CopyFrom(expr)

            if self.filter_expression.operator == FilterExpression.Operator.OR:
                request.filter_operator = nodereader_pb2.FilterOperator.OR
            else:
                request.filter_operator = nodereader_pb2.FilterOperator.AND

        if self.hidden is not None:
            expr = nodereader_pb2.FilterExpression()
            if self.hidden:
                expr.facet.facet = LABEL_HIDDEN
            else:
                expr.bool_not.facet.facet = LABEL_HIDDEN

            add_and_expression(request.field_filter, expr)

    def parse_sorting(self, request: nodereader_pb2.SearchRequest) -> None:
        if len(self.query) == 0:
            if self.sort is None:
                self.sort = SortOptions(
                    field=SortField.CREATED,
                    order=SortOrder.DESC,
                    limit=None,
                )
            elif self.sort.field not in INDEX_SORTABLE_FIELDS:
                raise InvalidQueryError(
                    "sort_field",
                    f"Empty query can only be sorted by '{SortField.CREATED}' or"
                    f" '{SortField.MODIFIED}' and sort limit won't be applied",
                )
        else:
            if self.sort is None:
                self.sort = SortOptions(
                    field=SortField.SCORE,
                    order=SortOrder.DESC,
                    limit=None,
                )
            elif self.sort.field not in INDEX_SORTABLE_FIELDS and self.sort.limit is None:
                raise InvalidQueryError(
                    "sort_field",
                    f"Sort by '{self.sort.field}' requires setting a sort limit",
                )

        # We need to ask for all and cut later
        request.page_number = 0
        if self.sort and self.sort.limit is not None:
            # As the index can't sort, we have to do it when merging. To
            # have consistent results, we must limit them
            request.result_per_page = self.sort.limit
        else:
            request.result_per_page = self.top_k

        sort_field = get_sort_field_proto(self.sort.field) if self.sort else None
        if sort_field is not None:
            request.order.sort_by = sort_field
            request.order.type = SortOrderMap[self.sort.order]  # type: ignore

    async def parse_min_score(self, request: nodereader_pb2.SearchRequest, incomplete: bool) -> None:
        semantic_min_score = DEFAULT_GENERIC_SEMANTIC_THRESHOLD
        if self.min_score.semantic is not None:
            semantic_min_score = self.min_score.semantic
        elif self.has_vector_search and not incomplete:
            query_information = await self._get_query_information()
            vectorset = await self.fetcher.get_vectorset()
            semantic_threshold = query_information.semantic_thresholds.get(vectorset, None)
            if semantic_threshold is not None:
                semantic_min_score = semantic_threshold
            else:
                logger.warning(
                    "Semantic threshold not found in query information, using default",
                    extra={"kbid": self.kbid},
                )
        self.min_score.semantic = semantic_min_score
        request.min_score_semantic = self.min_score.semantic
        request.min_score_bm25 = self.min_score.bm25

    def parse_document_search(self, request: nodereader_pb2.SearchRequest) -> None:
        if SearchOptions.FULLTEXT in self.features:
            request.document = True
            node_features.inc({"type": "documents"})

    def parse_paragraph_search(self, request: nodereader_pb2.SearchRequest) -> None:
        if SearchOptions.KEYWORD in self.features:
            request.paragraph = True
            node_features.inc({"type": "paragraphs"})

    async def parse_vector_search(
        self, request: nodereader_pb2.SearchRequest
    ) -> tuple[bool, Optional[str]]:
        if not self.has_vector_search:
            return False, None

        node_features.inc({"type": "vectors"})

        vectorset = await self.fetcher.get_vectorset()
        query_vector = await self.fetcher.get_query_vector()
        rephrased_query = await self.fetcher.get_rephrased_query()
        incomplete = query_vector is None

        request.vectorset = vectorset
        if query_vector is not None:
            request.vector.extend(query_vector)

        return incomplete, rephrased_query

    async def parse_relation_search(self, request: nodereader_pb2.SearchRequest) -> list[str]:
        autofilters = []
        # BUG: autofiler should autofilter, not enable relation search
        if self.has_relations_search or self.autofilter:
            if self.query_entities:
                detected_entities = []
                for entity in self.query_entities:
                    relation_node = utils_pb2.RelationNode()
                    relation_node.value = entity.name
                    if entity.type is not None:
                        relation_node.ntype = RelationNodeTypeMap[entity.type]
                    if entity.subtype is not None:
                        relation_node.subtype = entity.subtype
                    detected_entities.append(relation_node)
            else:
                detected_entities = await self.fetcher.get_detected_entities()
            meta_cache = await self.fetcher.get_entities_meta_cache()
            detected_entities = expand_entities(meta_cache, detected_entities)
            if self.has_relations_search:
                request.relation_subgraph.entry_points.extend(detected_entities)
                request.relation_subgraph.depth = 1
                request.relation_subgraph.deleted_groups.extend(
                    await self.fetcher.get_deleted_entity_groups()
                )
                for group_id, deleted_entities in meta_cache.deleted_entities.items():
                    request.relation_subgraph.deleted_entities.append(
                        nodereader_pb2.EntitiesSubgraphRequest.DeletedEntities(
                            node_subtype=group_id, node_values=deleted_entities
                        )
                    )
                node_features.inc({"type": "relations"})
            if self.autofilter:
                entity_filters = apply_entities_filter(request, detected_entities)
                autofilters.extend([translate_system_to_alias_label(e) for e in entity_filters])
        return autofilters

    async def parse_synonyms(self, request: nodereader_pb2.SearchRequest) -> None:
        """
        Replace the terms in the query with an expression that will make it match with the configured synonyms.
        We're using the Tantivy's query language here: https://docs.rs/tantivy/latest/tantivy/query/struct.QueryParser.html

        Example:
        - Synonyms: Foo -> Bar, Baz
        - Query: "What is Foo?"
        - Advanced Query: "What is (Foo OR Bar OR Baz)?"
        """
        if not self.with_synonyms or not self.query:
            # Nothing to do
            return

        if self.has_vector_search or self.has_relations_search:
            raise InvalidQueryError(
                "synonyms",
                "Search with custom synonyms is only supported on paragraph and document search",
            )

        synonyms = await self.fetcher.get_synonyms()
        if synonyms is None:
            # No synonyms found
            return

        # Calculate term variants: 'term' -> '(term OR synonym1 OR synonym2)'
        variants: dict[str, str] = {}
        for term, term_synonyms in synonyms.terms.items():
            if len(term_synonyms.synonyms) > 0:
                variants[term] = "({})".format(" OR ".join([term] + list(term_synonyms.synonyms)))

        # Split the query into terms
        query_terms = self.query.split()

        # Remove punctuation from the query terms
        clean_query_terms = [term.strip(string.punctuation) for term in query_terms]

        # Replace the original terms with the variants if the cleaned term is in the variants
        term_with_synonyms_found = False
        for index, clean_term in enumerate(clean_query_terms):
            if clean_term in variants:
                term_with_synonyms_found = True
                query_terms[index] = query_terms[index].replace(clean_term, variants[clean_term])

        if term_with_synonyms_found:
            request.advanced_query = " ".join(query_terms)
            request.ClearField("body")

    async def get_visual_llm_enabled(self) -> bool:
        return (await self._get_query_information()).visual_llm

    async def get_max_tokens_context(self) -> int:
        model_max = (await self._get_query_information()).max_context
        if self.max_tokens is not None and self.max_tokens.context is not None:
            if self.max_tokens.context > model_max:
                raise InvalidQueryError(
                    "max_tokens.context",
                    f"Max context tokens is higher than the model's limit of {model_max}",
                )
            return self.max_tokens.context
        return model_max

    def get_max_tokens_answer(self) -> Optional[int]:
        if self.max_tokens is not None and self.max_tokens.answer is not None:
            return self.max_tokens.answer
        return None

    async def adjust_page_size(
        self,
        request: nodereader_pb2.SearchRequest,
        rank_fusion: Optional[RankFusionAlgorithm],
        reranker: Optional[Reranker],
    ):
        """Adjust requested page size depending on rank fusion and reranking algorithms.

        Some rerankers want more results than the requested by the user so
        reranking can have more choices.

        """
        rank_fusion_window = 0
        if rank_fusion is not None:
            rank_fusion_window = rank_fusion.window

        reranker_window = 0
        if reranker is not None:
            reranker_window = reranker.window or 0

        request.result_per_page = max(
            request.result_per_page,
            rank_fusion_window,
            reranker_window,
        )


async def paragraph_query_to_pb(
    kbid: str,
    rid: str,
    query: str,
    filter_expression: Optional[FilterExpression],
    fields: list[str],
    filters: list[str],
    faceted: list[str],
    top_k: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    sort: Optional[str] = None,
    sort_ord: str = SortOrder.DESC.value,
    with_duplicates: bool = False,
) -> nodereader_pb2.SearchRequest:
    request = nodereader_pb2.SearchRequest()
    request.paragraph = True

    # We need to ask for all and cut later
    request.page_number = 0
    request.result_per_page = top_k

    request.body = query

    old = OldFilterParams(
        label_filters=filters,
        keyword_filters=[],
        range_creation_start=range_creation_start,
        range_creation_end=range_creation_end,
        range_modification_start=range_modification_start,
        range_modification_end=range_modification_end,
        fields=fields,
    )
    fetcher = Fetcher(
        kbid,
        query="",
        user_vector=None,
        vectorset=None,
        rephrase=False,
        rephrase_prompt=None,
        generative_model=None,
    )
    field_expr, paragraph_expr = await parse_old_filters(old, fetcher)
    if field_expr is not None:
        request.field_filter.CopyFrom(field_expr)
    if paragraph_expr is not None:
        request.paragraph_filter.CopyFrom(paragraph_expr)

    if (field_expr is not None or paragraph_expr is not None) and filter_expression is not None:
        raise InvalidQueryError("filter_expression", "Cannot mix old filters with filter_expression")

    if filter_expression:
        if filter_expression.field:
            expr = await parse_expression(filter_expression.field, kbid)
            if expr:
                request.field_filter.CopyFrom(expr)

        if filter_expression.paragraph:
            expr = await parse_expression(filter_expression.paragraph, kbid)
            if expr:
                request.paragraph_filter.CopyFrom(expr)

        if filter_expression.operator == FilterExpression.Operator.OR:
            request.filter_operator = nodereader_pb2.FilterOperator.OR
        else:
            request.filter_operator = nodereader_pb2.FilterOperator.AND

    key_filter = nodereader_pb2.FilterExpression()
    key_filter.resource.resource_id = rid
    add_and_expression(request.field_filter, key_filter)

    return request


def expand_entities(
    meta_cache: datamanagers.entities.EntitiesMetaCache,
    detected_entities: list[utils_pb2.RelationNode],
) -> list[utils_pb2.RelationNode]:
    """
    Iterate through duplicated entities in a kb.

    The algorithm first makes it so we can look up duplicates by source and
    by the referenced entity and expands from both directions.
    """
    result_entities = {entity.value: entity for entity in detected_entities}
    duplicated_entities = meta_cache.duplicate_entities
    duplicated_entities_by_value = meta_cache.duplicate_entities_by_value

    for entity in detected_entities[:]:
        if entity.subtype not in duplicated_entities:
            continue

        if entity.value in duplicated_entities[entity.subtype]:
            for duplicate in duplicated_entities[entity.subtype][entity.value]:
                result_entities[duplicate] = utils_pb2.RelationNode(
                    ntype=utils_pb2.RelationNode.NodeType.ENTITY,
                    subtype=entity.subtype,
                    value=duplicate,
                )

        if entity.value in duplicated_entities_by_value[entity.subtype]:
            source_duplicate = duplicated_entities_by_value[entity.subtype][entity.value]
            result_entities[source_duplicate] = utils_pb2.RelationNode(
                ntype=utils_pb2.RelationNode.NodeType.ENTITY,
                subtype=entity.subtype,
                value=source_duplicate,
            )

            if source_duplicate in duplicated_entities[entity.subtype]:
                for duplicate in duplicated_entities[entity.subtype][source_duplicate]:
                    if duplicate == entity.value:
                        continue
                    result_entities[duplicate] = utils_pb2.RelationNode(
                        ntype=utils_pb2.RelationNode.NodeType.ENTITY,
                        subtype=entity.subtype,
                        value=duplicate,
                    )

    return list(result_entities.values())


def apply_entities_filter(
    request: nodereader_pb2.SearchRequest,
    detected_entities: list[utils_pb2.RelationNode],
) -> list[str]:
    added_filters = []
    for entity_filter in [
        f"/e/{entity.subtype}/{entity.value}"
        for entity in detected_entities
        if entity.ntype == utils_pb2.RelationNode.NodeType.ENTITY
    ]:
        if entity_filter not in added_filters:
            added_filters.append(entity_filter)
            # Add the entity to the filter expression (with AND)
            entity_expr = nodereader_pb2.FilterExpression()
            entity_expr.facet.facet = translate_label(entity_filter)
            add_and_expression(request.field_filter, entity_expr)

    return added_filters


async def suggest_query_to_pb(
    kbid: str,
    features: list[SuggestOptions],
    query: str,
    filter_expression: Optional[FilterExpression],
    fields: list[str],
    filters: list[str],
    faceted: list[str],
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    hidden: Optional[bool] = None,
) -> nodereader_pb2.SuggestRequest:
    request = nodereader_pb2.SuggestRequest()

    request.body = query
    if SuggestOptions.ENTITIES in features:
        request.features.append(nodereader_pb2.SuggestFeatures.ENTITIES)

    if SuggestOptions.PARAGRAPH in features:
        request.features.append(nodereader_pb2.SuggestFeatures.PARAGRAPHS)

    old = OldFilterParams(
        label_filters=filters,
        keyword_filters=[],
        range_creation_start=range_creation_start,
        range_creation_end=range_creation_end,
        range_modification_start=range_modification_start,
        range_modification_end=range_modification_end,
        fields=fields,
    )
    fetcher = Fetcher(
        kbid,
        query="",
        user_vector=None,
        vectorset=None,
        rephrase=False,
        rephrase_prompt=None,
        generative_model=None,
    )
    field_expr, _ = await parse_old_filters(old, fetcher)
    if field_expr is not None and filter_expression is not None:
        raise InvalidQueryError("filter_expression", "Cannot mix old filters with filter_expression")

    if field_expr is not None:
        request.field_filter.CopyFrom(field_expr)

    if filter_expression:
        if filter_expression.field:
            expr = await parse_expression(filter_expression.field, kbid)
            if expr:
                request.field_filter.CopyFrom(expr)

        if filter_expression.paragraph:
            expr = await parse_expression(filter_expression.paragraph, kbid)
            if expr:
                request.paragraph_filter.CopyFrom(expr)

        if filter_expression.operator == FilterExpression.Operator.OR:
            request.filter_operator = nodereader_pb2.FilterOperator.OR
        else:
            request.filter_operator = nodereader_pb2.FilterOperator.AND

    if hidden is not None:
        expr = nodereader_pb2.FilterExpression()
        if hidden:
            expr.facet.facet = LABEL_HIDDEN
        else:
            expr.bool_not.facet.facet = LABEL_HIDDEN

        add_and_expression(request.field_filter, expr)

    return request


PROCESSING_STATUS_TO_PB_MAP = {
    ResourceProcessingStatus.PENDING: Resource.ResourceStatus.PENDING,
    ResourceProcessingStatus.PROCESSED: Resource.ResourceStatus.PROCESSED,
    ResourceProcessingStatus.ERROR: Resource.ResourceStatus.ERROR,
    ResourceProcessingStatus.EMPTY: Resource.ResourceStatus.EMPTY,
    ResourceProcessingStatus.BLOCKED: Resource.ResourceStatus.BLOCKED,
    ResourceProcessingStatus.EXPIRED: Resource.ResourceStatus.EXPIRED,
}


def check_supported_filters(filters: dict[str, Any], paragraph_labels: list[str]):
    """
    Check if the provided filters are supported:
    Paragraph labels can only be used with simple 'and' expressions (not nested).
    """
    if len(paragraph_labels) == 0:
        return
    if "literal" in filters:
        return
    if "and" not in filters:
        # Paragraph labels can only be used with 'and' filter
        raise InvalidQueryError(
            "filters",
            "Paragraph labels can only be used with 'all' filter",
        )
    for term in filters["and"]:
        # Nested expressions are not allowed with paragraph labels (only "literal" and "not(literal)")
        if "not" in term:
            subterm = term["not"]
            if "literal" not in subterm:
                # AND (NOT( X )) where X is anything other than a literal
                raise InvalidQueryError(
                    "filters",
                    "Paragraph labels can only be used with 'all' filter",
                )
        elif "literal" not in term:
            raise InvalidQueryError(
                "filters",
                "Paragraph labels can only be used with 'all' filter",
            )


def get_sort_field_proto(obj: SortField) -> Optional[nodereader_pb2.OrderBy.OrderField.ValueType]:
    return {
        SortField.SCORE: None,
        SortField.CREATED: nodereader_pb2.OrderBy.OrderField.CREATED,
        SortField.MODIFIED: nodereader_pb2.OrderBy.OrderField.MODIFIED,
        SortField.TITLE: None,
    }[obj]
