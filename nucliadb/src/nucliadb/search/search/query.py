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
import json
import string
from datetime import datetime
from typing import Any, Awaitable, Optional, Union

from async_lru import alru_cache

from nucliadb.common import datamanagers
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search import logger
from nucliadb.search.predict import SendToPredictError, convert_relations
from nucliadb.search.search.filters import (
    convert_to_node_filters,
    flatten_filter_literals,
    has_classification_label_filters,
    split_labels_by_type,
    translate_label,
    translate_label_filters,
)
from nucliadb.search.search.metrics import (
    node_features,
    query_parse_dependency_observer,
)
from nucliadb.search.search.rank_fusion import (
    RankFusionAlgorithm,
)
from nucliadb.search.search.rerankers import (
    Reranker,
)
from nucliadb.search.utilities import get_predict
from nucliadb_models.internal.predict import QueryInfo
from nucliadb_models.labels import LABEL_HIDDEN, translate_system_to_alias_label
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    Filter,
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
from nucliadb_protos import knowledgebox_pb2, nodereader_pb2, utils_pb2
from nucliadb_protos.noderesources_pb2 import Resource

from .exceptions import InvalidQueryError

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
    _get_vectorset_task: Optional[asyncio.Task] = None
    _detected_entities_task: Optional[asyncio.Task] = None
    _entities_meta_cache_task: Optional[asyncio.Task] = None
    _deleted_entities_groups_task: Optional[asyncio.Task] = None
    _synonyms_task: Optional[asyncio.Task] = None
    _get_classification_labels_task: Optional[asyncio.Task] = None
    _get_matryoshka_dimension_task: Optional[asyncio.Task] = None

    def __init__(
        self,
        *,
        kbid: str,
        features: list[SearchOptions],
        query: str,
        label_filters: Union[list[str], list[Filter]],
        keyword_filters: Union[list[str], list[Filter]],
        top_k: int,
        min_score: MinScore,
        faceted: Optional[list[str]] = None,
        sort: Optional[SortOptions] = None,
        range_creation_start: Optional[datetime] = None,
        range_creation_end: Optional[datetime] = None,
        range_modification_start: Optional[datetime] = None,
        range_modification_end: Optional[datetime] = None,
        fields: Optional[list[str]] = None,
        user_vector: Optional[list[float]] = None,
        vectorset: Optional[str] = None,
        with_duplicates: bool = False,
        with_status: Optional[ResourceProcessingStatus] = None,
        with_synonyms: bool = False,
        autofilter: bool = False,
        key_filters: Optional[list[str]] = None,
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
        self.hidden = hidden
        if self.hidden is not None:
            if self.hidden:
                label_filters.append(Filter(all=[LABEL_HIDDEN]))  # type: ignore
            else:
                label_filters.append(Filter(none=[LABEL_HIDDEN]))  # type: ignore

        self.label_filters: dict[str, Any] = convert_to_node_filters(label_filters)
        self.flat_label_filters: list[str] = []
        self.keyword_filters: dict[str, Any] = convert_to_node_filters(keyword_filters)
        self.faceted = faceted or []
        self.top_k = top_k
        self.min_score = min_score
        self.sort = sort
        self.range_creation_start = range_creation_start
        self.range_creation_end = range_creation_end
        self.range_modification_start = range_modification_start
        self.range_modification_end = range_modification_end
        self.fields = fields or []
        self.user_vector = user_vector
        self.vectorset = vectorset
        self.with_duplicates = with_duplicates
        self.with_status = with_status
        self.with_synonyms = with_synonyms
        self.autofilter = autofilter
        self.key_filters = key_filters
        self.security = security
        self.generative_model = generative_model
        self.rephrase = rephrase
        self.rephrase_prompt = rephrase_prompt
        self.query_endpoint_used = False
        if len(self.label_filters) > 0:
            self.label_filters = translate_label_filters(self.label_filters)
            self.flat_label_filters = flatten_filter_literals(self.label_filters)
        self.max_tokens = max_tokens
        self.rank_fusion = rank_fusion
        self.reranker = reranker

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
        vectorset = await self.select_query_vectorset()
        return await query_information(
            self.kbid, self.query, vectorset, self.generative_model, self.rephrase, self.rephrase_prompt
        )

    def _get_vectorset(self) -> Awaitable[Optional[str]]:
        if self._get_vectorset_task is None:
            self._get_vectorset_task = asyncio.create_task(self._select_vectorset())
        return self._get_vectorset_task

    async def _select_vectorset(self) -> Optional[str]:
        if self.vectorset:
            return self.vectorset

        # When vectorset is not provided we get the default from Predict API

        try:
            query_information = await self._get_query_information()
        except SendToPredictError:
            return None

        if query_information.sentence is None:
            logger.error(
                "Asking for a vectorset but /query didn't return one", extra={"kbid": self.kbid}
            )
            return None

        for vectorset in query_information.sentence.vectors.keys():
            self.vectorset = vectorset
            break

        return self.vectorset

    def _get_matryoshka_dimension(self) -> Awaitable[Optional[int]]:
        if self._get_matryoshka_dimension_task is None:
            self._get_matryoshka_dimension_task = asyncio.create_task(self._matryoshka_dimension())
        return self._get_matryoshka_dimension_task

    async def _matryoshka_dimension(self) -> Optional[int]:
        vectorset = await self._select_vectorset()
        return await get_matryoshka_dimension_cached(self.kbid, vectorset)

    def _get_detected_entities(self) -> Awaitable[list[utils_pb2.RelationNode]]:
        if self._detected_entities_task is None:  # pragma: no cover
            self._detected_entities_task = asyncio.create_task(detect_entities(self.kbid, self.query))
        return self._detected_entities_task

    def _get_entities_meta_cache(
        self,
    ) -> Awaitable[datamanagers.entities.EntitiesMetaCache]:
        if self._entities_meta_cache_task is None:
            self._entities_meta_cache_task = asyncio.create_task(get_entities_meta_cache(self.kbid))
        return self._entities_meta_cache_task

    def _get_deleted_entity_groups(self) -> Awaitable[list[str]]:
        if self._deleted_entities_groups_task is None:
            self._deleted_entities_groups_task = asyncio.create_task(
                get_deleted_entity_groups(self.kbid)
            )
        return self._deleted_entities_groups_task

    def _get_synomyns(self) -> Awaitable[Optional[knowledgebox_pb2.Synonyms]]:
        if self._synonyms_task is None:
            self._synonyms_task = asyncio.create_task(get_kb_synonyms(self.kbid))
        return self._synonyms_task

    def _get_classification_labels(self) -> Awaitable[knowledgebox_pb2.Labels]:
        if self._get_classification_labels_task is None:
            self._get_classification_labels_task = asyncio.create_task(
                get_classification_labels(self.kbid)
            )
        return self._get_classification_labels_task

    async def _schedule_dependency_tasks(self) -> None:
        """
        This will schedule concurrent tasks for different data that needs to be pulled
        for the sake of the query being performed
        """
        if len(self.label_filters) > 0 and has_classification_label_filters(self.flat_label_filters):
            asyncio.ensure_future(self._get_classification_labels())

        if self.has_vector_search and self.user_vector is None:
            self.query_endpoint_used = True
            asyncio.ensure_future(self._get_query_information())
            asyncio.ensure_future(self._get_matryoshka_dimension())

        if (self.has_relations_search or self.autofilter) and len(self.query) > 0:
            if not self.query_endpoint_used:
                # If we only need to detect entities, we don't need the query endpoint
                asyncio.ensure_future(self._get_detected_entities())
            asyncio.ensure_future(self._get_entities_meta_cache())
            asyncio.ensure_future(self._get_deleted_entity_groups())
        if self.with_synonyms and self.query:
            asyncio.ensure_future(self._get_synomyns())

    async def parse(self) -> tuple[nodereader_pb2.SearchRequest, bool, list[str]]:
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
        incomplete = await self.parse_vector_search(request)
        autofilters = await self.parse_relation_search(request)
        await self.parse_synonyms(request)
        await self.parse_min_score(request, incomplete)
        await self.adjust_page_size(request, self.rank_fusion, self.reranker)
        return request, incomplete, autofilters

    async def parse_filters(self, request: nodereader_pb2.SearchRequest) -> None:
        if len(self.label_filters) > 0:
            field_labels = self.flat_label_filters
            paragraph_labels: list[str] = []
            if has_classification_label_filters(self.flat_label_filters):
                classification_labels = await self._get_classification_labels()
                field_labels, paragraph_labels = split_labels_by_type(
                    self.flat_label_filters, classification_labels
                )
                check_supported_filters(self.label_filters, paragraph_labels)

            request.filter.field_labels.extend(field_labels)
            request.filter.paragraph_labels.extend(paragraph_labels)
            request.filter.labels_expression = json.dumps(self.label_filters)

        if len(self.keyword_filters) > 0:
            request.filter.keywords_expression = json.dumps(self.keyword_filters)

        request.faceted.labels.extend([translate_label(facet) for facet in self.faceted])
        request.fields.extend(self.fields)

        if self.security is not None and len(self.security.groups) > 0:
            security_pb = utils_pb2.Security()
            for group_id in self.security.groups:
                if group_id not in security_pb.access_groups:
                    security_pb.access_groups.append(group_id)
            request.security.CopyFrom(security_pb)

        if self.key_filters is not None and len(self.key_filters) > 0:
            request.key_filters.extend(self.key_filters)
            node_features.inc({"type": "key_filters"})

        if self.with_status is not None:
            request.with_status = PROCESSING_STATUS_TO_PB_MAP[self.with_status]

        if self.range_creation_start is not None:
            request.timestamps.from_created.FromDatetime(self.range_creation_start)

        if self.range_creation_end is not None:
            request.timestamps.to_created.FromDatetime(self.range_creation_end)

        if self.range_modification_start is not None:
            request.timestamps.from_modified.FromDatetime(self.range_modification_start)

        if self.range_modification_end is not None:
            request.timestamps.to_modified.FromDatetime(self.range_modification_end)

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
            vectorset = await self._select_vectorset()
            if vectorset is not None:
                semantic_threshold = query_information.semantic_thresholds.get(vectorset, None)
                if semantic_threshold is not None:
                    semantic_min_score = semantic_threshold
                else:
                    logger.warning(
                        "Semantic threshold not found in query information, using default",
                        extra={"kbid": self.kbid},
                    )
            else:
                logger.warning(
                    "Vectorset unset by user or predict, using default semantic threshold",
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

    async def select_query_vectorset(self) -> Optional[str]:
        """Set and return the requested vectorset parameter (if used) validated
        for the current KB.

        """
        if not self.vectorset:
            return None

        # validate vectorset
        async with datamanagers.with_ro_transaction() as txn:
            if not await datamanagers.vectorsets.exists(
                txn, kbid=self.kbid, vectorset_id=self.vectorset
            ):
                raise InvalidQueryError(
                    "vectorset",
                    f"Vectorset {self.vectorset} doesn't exist in you Knowledge Box",
                )
        return self.vectorset

    async def parse_vector_search(self, request: nodereader_pb2.SearchRequest) -> bool:
        if not self.has_vector_search:
            return False

        node_features.inc({"type": "vectors"})

        incomplete = False

        vectorset = await self._select_vectorset()
        if vectorset is not None:
            request.vectorset = vectorset

        query_vector = None
        if self.user_vector is None:
            try:
                query_info = await self._get_query_information()
            except SendToPredictError as err:
                logger.warning(f"Errors on predict api trying to embedd query: {err}")
                incomplete = True
            else:
                if query_info and query_info.sentence:
                    if vectorset:
                        if vectorset in query_info.sentence.vectors:
                            query_vector = query_info.sentence.vectors[vectorset]
                        else:
                            incomplete = True
                    else:
                        for vectorset_id, vector in query_info.sentence.vectors.items():
                            if vector:
                                query_vector = vector
                                break
                        else:
                            incomplete = True

                else:
                    incomplete = True
        else:
            query_vector = self.user_vector

        if query_vector is not None:
            matryoshka_dimension = await self._get_matryoshka_dimension()
            if matryoshka_dimension is not None:
                # KB using a matryoshka embeddings model, cut the query vector
                # accordingly
                query_vector = query_vector[:matryoshka_dimension]
            request.vector.extend(query_vector)

        return incomplete

    async def parse_relation_search(self, request: nodereader_pb2.SearchRequest) -> list[str]:
        autofilters = []
        if self.has_relations_search or self.autofilter:
            if not self.query_endpoint_used:
                detected_entities = await self._get_detected_entities()
            else:
                query_info_result = await self._get_query_information()
                if query_info_result.entities:
                    detected_entities = convert_relations(query_info_result.entities.model_dump())
                else:
                    detected_entities = []
            meta_cache = await self._get_entities_meta_cache()
            detected_entities = expand_entities(meta_cache, detected_entities)
            if self.has_relations_search:
                request.relation_subgraph.entry_points.extend(detected_entities)
                request.relation_subgraph.depth = 1
                request.relation_subgraph.deleted_groups.extend(await self._get_deleted_entity_groups())
                for group_id, deleted_entities in meta_cache.deleted_entities.items():
                    request.relation_subgraph.deleted_entities.append(
                        nodereader_pb2.EntitiesSubgraphRequest.DeletedEntities(
                            node_subtype=group_id, node_values=deleted_entities
                        )
                    )
                node_features.inc({"type": "relations"})
            if self.autofilter:
                entity_filters = parse_entities_to_filters(request, detected_entities)
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

        synonyms = await self._get_synomyns()
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

    # we don't have a specific filter only for resource_ids but key_filters
    # parse "rid" and "rid/field" like ids, so it does the job
    request.key_filters.append(rid)

    if len(filters) > 0:
        field_labels = filters
        paragraph_labels: list[str] = []
        if has_classification_label_filters(filters):
            classification_labels = await get_classification_labels(kbid)
            field_labels, paragraph_labels = split_labels_by_type(filters, classification_labels)
        request.filter.field_labels.extend(field_labels)
        request.filter.paragraph_labels.extend(paragraph_labels)

    request.faceted.labels.extend([translate_label(facet) for facet in faceted])
    request.fields.extend(fields)

    if sort:
        request.order.field = sort
        request.order.type = sort_ord  # type: ignore

    request.with_duplicates = with_duplicates

    if range_creation_start is not None:
        request.timestamps.from_created.FromDatetime(range_creation_start)

    if range_creation_end is not None:
        request.timestamps.to_created.FromDatetime(range_creation_end)

    if range_modification_start is not None:
        request.timestamps.from_modified.FromDatetime(range_modification_start)

    if range_modification_end is not None:
        request.timestamps.to_modified.FromDatetime(range_modification_end)

    return request


@query_parse_dependency_observer.wrap({"type": "query_information"})
async def query_information(
    kbid: str,
    query: str,
    semantic_model: Optional[str],
    generative_model: Optional[str] = None,
    rephrase: bool = False,
    rephrase_prompt: Optional[str] = None,
) -> QueryInfo:
    predict = get_predict()
    return await predict.query(kbid, query, semantic_model, generative_model, rephrase, rephrase_prompt)


@query_parse_dependency_observer.wrap({"type": "detect_entities"})
async def detect_entities(kbid: str, query: str) -> list[utils_pb2.RelationNode]:
    predict = get_predict()
    try:
        return await predict.detect_entities(kbid, query)
    except SendToPredictError as ex:
        logger.warning(f"Errors on predict api detecting entities: {ex}")
        return []


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


def parse_entities_to_filters(
    request: nodereader_pb2.SearchRequest,
    detected_entities: list[utils_pb2.RelationNode],
) -> list[str]:
    added_filters = []
    for entity_filter in [
        f"/e/{entity.subtype}/{entity.value}"
        for entity in detected_entities
        if entity.ntype == utils_pb2.RelationNode.NodeType.ENTITY
    ]:
        if entity_filter not in request.filter.field_labels:
            request.filter.field_labels.append(entity_filter)
            added_filters.append(entity_filter)

    # We need to expand the filter expression with the automatically detected entities.
    if len(added_filters) > 0:
        # So far, autofilters feature will only yield 'and' expressions with the detected entities.
        # More complex autofilters can be added here if we leverage the query endpoint.
        expanded_expression = {"and": [{"literal": entity} for entity in added_filters]}
        if request.filter.labels_expression:
            expression = json.loads(request.filter.labels_expression)
            expanded_expression["and"].append(expression)
        request.filter.labels_expression = json.dumps(expanded_expression)
    return added_filters


def suggest_query_to_pb(
    features: list[SuggestOptions],
    query: str,
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
        request.fields.extend(fields)

        if hidden is not None:
            if hidden:
                filters.append(Filter(all=[LABEL_HIDDEN]))  # type: ignore
            else:
                filters.append(Filter(none=[LABEL_HIDDEN]))  # type: ignore

        expression = convert_to_node_filters(filters)
        if expression:
            expression = translate_label_filters(expression)

        request.filter.field_labels.extend(flatten_filter_literals(expression))
        request.filter.labels_expression = json.dumps(expression)

    if range_creation_start is not None:
        request.timestamps.from_created.FromDatetime(range_creation_start)
    if range_creation_end is not None:
        request.timestamps.to_created.FromDatetime(range_creation_end)
    if range_modification_start is not None:
        request.timestamps.from_modified.FromDatetime(range_modification_start)
    if range_modification_end is not None:
        request.timestamps.to_modified.FromDatetime(range_modification_end)

    return request


PROCESSING_STATUS_TO_PB_MAP = {
    ResourceProcessingStatus.PENDING: Resource.ResourceStatus.PENDING,
    ResourceProcessingStatus.PROCESSED: Resource.ResourceStatus.PROCESSED,
    ResourceProcessingStatus.ERROR: Resource.ResourceStatus.ERROR,
    ResourceProcessingStatus.EMPTY: Resource.ResourceStatus.EMPTY,
    ResourceProcessingStatus.BLOCKED: Resource.ResourceStatus.BLOCKED,
    ResourceProcessingStatus.EXPIRED: Resource.ResourceStatus.EXPIRED,
}


@query_parse_dependency_observer.wrap({"type": "synonyms"})
async def get_kb_synonyms(kbid: str) -> Optional[knowledgebox_pb2.Synonyms]:
    async with get_driver().transaction(read_only=True) as txn:
        return await datamanagers.synonyms.get(txn, kbid=kbid)


@query_parse_dependency_observer.wrap({"type": "entities_meta_cache"})
async def get_entities_meta_cache(kbid: str) -> datamanagers.entities.EntitiesMetaCache:
    async with get_driver().transaction(read_only=True) as txn:
        return await datamanagers.entities.get_entities_meta_cache(txn, kbid=kbid)


@query_parse_dependency_observer.wrap({"type": "deleted_entities_groups"})
async def get_deleted_entity_groups(kbid: str) -> list[str]:
    async with get_driver().transaction(read_only=True) as txn:
        return list((await datamanagers.entities.get_deleted_groups(txn, kbid=kbid)).entities_groups)


@query_parse_dependency_observer.wrap({"type": "classification_labels"})
async def get_classification_labels(kbid: str) -> knowledgebox_pb2.Labels:
    async with get_driver().transaction(read_only=True) as txn:
        return await datamanagers.labels.get_labels(txn, kbid=kbid)


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


@alru_cache(maxsize=None)
async def get_matryoshka_dimension_cached(kbid: str, vectorset: Optional[str]) -> Optional[int]:
    # This can be safely cached as the matryoshka dimension is not expected to change
    return await get_matryoshka_dimension(kbid, vectorset)


@query_parse_dependency_observer.wrap({"type": "matryoshka_dimension"})
async def get_matryoshka_dimension(kbid: str, vectorset: Optional[str]) -> Optional[int]:
    async with get_driver().transaction(read_only=True) as txn:
        matryoshka_dimension = None
        if not vectorset:
            # XXX this should be migrated once we remove the "default" vectorset
            # concept
            matryoshka_dimension = await datamanagers.kb.get_matryoshka_vector_dimension(txn, kbid=kbid)
        else:
            vectorset_config = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset)
            if vectorset_config is not None and vectorset_config.vectorset_index_config.vector_dimension:
                matryoshka_dimension = vectorset_config.vectorset_index_config.vector_dimension

        return matryoshka_dimension


def get_sort_field_proto(obj: SortField) -> Optional[nodereader_pb2.OrderBy.OrderField.ValueType]:
    return {
        SortField.SCORE: None,
        SortField.CREATED: nodereader_pb2.OrderBy.OrderField.CREATED,
        SortField.MODIFIED: nodereader_pb2.OrderBy.OrderField.MODIFIED,
        SortField.TITLE: None,
    }[obj]
