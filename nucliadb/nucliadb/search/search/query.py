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
from datetime import datetime
from typing import Any, Awaitable, Optional, Union

from async_lru import alru_cache
from nucliadb_protos.noderesources_pb2 import Resource

from nucliadb.common.datamanagers.entities import EntitiesDataManager, EntitiesMetaCache
from nucliadb.common.datamanagers.kb import KnowledgeBoxDataManager
from nucliadb.common.datamanagers.labels import LabelsDataManager
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.synonyms import Synonyms
from nucliadb.middleware.transaction import get_read_only_transaction
from nucliadb.search import logger
from nucliadb.search.predict import PredictVectorMissing, SendToPredictError
from nucliadb.search.search.filters import (
    convert_to_node_filters,
    flat_filter_labels,
    has_classification_label_filters,
    split_labels_by_type,
    translate_label,
    translate_label_filters,
)
from nucliadb.search.search.metrics import (
    node_features,
    query_parse_dependency_observer,
)
from nucliadb.search.utilities import get_predict
from nucliadb_models.labels import translate_system_to_alias_label
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    Filter,
    MinScore,
    SearchOptions,
    SortField,
    SortFieldMap,
    SortOptions,
    SortOrder,
    SortOrderMap,
    SuggestOptions,
)
from nucliadb_models.security import RequestSecurity
from nucliadb_protos import knowledgebox_pb2, nodereader_pb2, utils_pb2

from .exceptions import InvalidQueryError

INDEX_SORTABLE_FIELDS = [
    SortField.CREATED,
    SortField.MODIFIED,
]


class QueryParser:
    """
    Queries are getting more and more complex and different phases of the query
    depending on different data.

    This class is an encapsulation of the different phases of the query and allow
    some stateful interaction with a query and different depenedencies during
    query parsing.
    """

    _min_score_task: Optional[asyncio.Task] = None
    _convert_vectors_task: Optional[asyncio.Task] = None
    _detected_entities_task: Optional[asyncio.Task] = None
    _entities_meta_cache_task: Optional[asyncio.Task] = None
    _deleted_entities_groups_task: Optional[asyncio.Task] = None
    _synonyms_task: Optional[asyncio.Task] = None
    _get_classification_labels_task: Optional[asyncio.Task] = None

    def __init__(
        self,
        *,
        kbid: str,
        features: list[SearchOptions],
        query: str,
        filters: Union[list[str], list[Filter]],
        page_number: int,
        page_size: int,
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
    ):
        self.kbid = kbid
        self.features = features
        self.query = query
        self.filters: dict[str, Any] = convert_to_node_filters(filters)
        self.flat_filter_labels: list[str] = []
        self.faceted = faceted or []
        self.page_number = page_number
        self.page_size = page_size
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

        if len(self.filters) > 0:
            self.filters = translate_label_filters(self.filters)
            self.flat_filter_labels = flat_filter_labels(self.filters)

    def _get_default_semantic_min_score(self) -> Awaitable[float]:
        if self._min_score_task is None:  # pragma: no cover
            self._min_score_task = asyncio.create_task(
                get_default_semantic_min_score(self.kbid)
            )
        return self._min_score_task

    def _get_converted_vectors(self) -> Awaitable[list[float]]:
        if self._convert_vectors_task is None:  # pragma: no cover
            self._convert_vectors_task = asyncio.create_task(
                convert_vectors(self.kbid, self.query)
            )
        return self._convert_vectors_task

    def _get_detected_entities(self) -> Awaitable[list[utils_pb2.RelationNode]]:
        if self._detected_entities_task is None:  # pragma: no cover
            self._detected_entities_task = asyncio.create_task(
                detect_entities(self.kbid, self.query)
            )
        return self._detected_entities_task

    def _get_entities_meta_cache(self) -> Awaitable[EntitiesMetaCache]:
        if self._entities_meta_cache_task is None:
            self._entities_meta_cache_task = asyncio.create_task(
                get_entities_meta_cache(self.kbid)
            )
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
        if len(self.filters) > 0 and has_classification_label_filters(
            self.flat_filter_labels
        ):
            asyncio.ensure_future(self._get_classification_labels())
        if self.min_score.semantic is None:
            asyncio.ensure_future(self._get_default_semantic_min_score())
        if SearchOptions.VECTOR in self.features and self.user_vector is None:
            asyncio.ensure_future(self._get_converted_vectors())
        if (SearchOptions.RELATIONS in self.features or self.autofilter) and len(
            self.query
        ) > 0:
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

        await self._schedule_dependency_tasks()

        await self.parse_filters(request)
        self.parse_document_search(request)
        self.parse_paragraph_search(request)
        incomplete = await self.parse_vector_search(request)
        autofilters = await self.parse_relation_search(request)
        await self.parse_synonyms(request)

        self.parse_sorting(request)
        await self.parse_min_score(request)

        return request, incomplete, autofilters

    async def parse_filters(self, request: nodereader_pb2.SearchRequest) -> None:
        if len(self.filters) > 0:
            field_labels = self.flat_filter_labels
            paragraph_labels: list[str] = []
            if has_classification_label_filters(self.flat_filter_labels):
                classification_labels = await self._get_classification_labels()
                field_labels, paragraph_labels = split_labels_by_type(
                    self.flat_filter_labels, classification_labels
                )
                check_supported_filters(self.filters, paragraph_labels)

            request.filter.field_labels.extend(field_labels)
            request.filter.paragraph_labels.extend(paragraph_labels)
            request.filter.expression = json.dumps(self.filters)

        request.faceted.labels.extend(
            [translate_label(facet) for facet in self.faceted]
        )
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
            elif (
                self.sort.field not in INDEX_SORTABLE_FIELDS and self.sort.limit is None
            ):
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
            request.result_per_page = self.page_number * self.page_size + self.page_size

        sort_field = SortFieldMap[self.sort.field] if self.sort else None
        if sort_field is not None:
            request.order.sort_by = sort_field
            request.order.type = SortOrderMap[self.sort.order]  # type: ignore

    async def parse_min_score(self, request: nodereader_pb2.SearchRequest) -> None:
        if self.min_score.semantic is None:
            self.min_score.semantic = await self._get_default_semantic_min_score()
        request.min_score_semantic = self.min_score.semantic
        request.min_score_bm25 = self.min_score.bm25

    def parse_document_search(self, request: nodereader_pb2.SearchRequest) -> None:
        if SearchOptions.DOCUMENT in self.features:
            request.document = True
            node_features.inc({"type": "documents"})

    def parse_paragraph_search(self, request: nodereader_pb2.SearchRequest) -> None:
        if SearchOptions.PARAGRAPH in self.features:
            request.paragraph = True
            node_features.inc({"type": "paragraphs"})

    async def parse_vector_search(self, request: nodereader_pb2.SearchRequest) -> bool:
        if SearchOptions.VECTOR not in self.features:
            return False

        node_features.inc({"type": "vectors"})

        incomplete = False
        if self.vectorset is not None:
            request.vectorset = self.vectorset
            node_features.inc({"type": "vectorset"})

        if self.user_vector is None:
            try:
                request.vector.extend(await self._get_converted_vectors())
            except SendToPredictError as err:
                logger.warning(f"Errors on predict api trying to embedd query: {err}")
                incomplete = True
            except PredictVectorMissing:
                logger.warning("Predict api returned an empty vector")
                incomplete = True
        else:
            request.vector.extend(self.user_vector)
        return incomplete

    async def parse_relation_search(
        self, request: nodereader_pb2.SearchRequest
    ) -> list[str]:
        autofilters = []
        relations_search = SearchOptions.RELATIONS in self.features
        if relations_search or self.autofilter:
            detected_entities = await self._get_detected_entities()
            meta_cache = await self._get_entities_meta_cache()
            detected_entities = expand_entities(meta_cache, detected_entities)
            if relations_search:
                request.relation_subgraph.entry_points.extend(detected_entities)
                request.relation_subgraph.depth = 1
                request.relation_subgraph.deleted_groups.extend(
                    await self._get_deleted_entity_groups()
                )
                for group_id, deleted_entities in meta_cache.deleted_entities.items():
                    request.relation_subgraph.deleted_entities.append(
                        nodereader_pb2.EntitiesSubgraphRequest.DeletedEntities(
                            node_subtype=group_id, node_values=deleted_entities
                        )
                    )
                node_features.inc({"type": "relations"})
            if self.autofilter:
                entity_filters = parse_entities_to_filters(request, detected_entities)
                autofilters.extend(
                    [translate_system_to_alias_label(e) for e in entity_filters]
                )
        return autofilters

    async def parse_synonyms(self, request: nodereader_pb2.SearchRequest) -> None:
        if not self.with_synonyms:
            return

        if (
            SearchOptions.VECTOR in self.features
            or SearchOptions.RELATIONS in self.features
        ):
            raise InvalidQueryError(
                "synonyms",
                "Search with custom synonyms is only supported on paragraph and document search",
            )

        if not self.query:
            # Nothing to do
            return

        synonyms = await self._get_synomyns()
        if synonyms is None:
            # No synonyms found
            return

        synonyms_found: list[str] = []
        advanced_query = []
        for term in self.query.split(" "):
            advanced_query.append(term)
            term_synonyms = synonyms.terms.get(term)
            if term_synonyms is None or len(term_synonyms.synonyms) == 0:
                # No synonyms found for this term
                continue
            synonyms_found.extend(term_synonyms.synonyms)

        if len(synonyms_found):
            request.advanced_query = " OR ".join(advanced_query + synonyms_found)
            request.ClearField("body")


async def paragraph_query_to_pb(
    kbid: str,
    features: list[SearchOptions],
    rid: str,
    query: str,
    fields: list[str],
    filters: list[str],
    faceted: list[str],
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    sort: Optional[str] = None,
    sort_ord: str = SortOrder.DESC.value,
    with_duplicates: bool = False,
) -> nodereader_pb2.ParagraphSearchRequest:
    request = nodereader_pb2.ParagraphSearchRequest()
    request.with_duplicates = with_duplicates

    # We need to ask for all and cut later
    request.page_number = 0
    request.result_per_page = page_number * page_size + page_size

    if range_creation_start is not None:
        request.timestamps.from_created.FromDatetime(range_creation_start)

    if range_creation_end is not None:
        request.timestamps.to_created.FromDatetime(range_creation_end)

    if range_modification_start is not None:
        request.timestamps.from_modified.FromDatetime(range_modification_start)

    if range_modification_end is not None:
        request.timestamps.to_modified.FromDatetime(range_modification_end)

    if SearchOptions.PARAGRAPH in features:
        request.uuid = rid
        request.body = query
        if len(filters) > 0:
            field_labels = filters
            paragraph_labels: list[str] = []
            if has_classification_label_filters(filters):
                classification_labels = await get_classification_labels(kbid)
                field_labels, paragraph_labels = split_labels_by_type(
                    filters, classification_labels
                )
            request.filter.field_labels.extend(field_labels)
            request.filter.paragraph_labels.extend(paragraph_labels)

        request.faceted.labels.extend([translate_label(facet) for facet in faceted])
        if sort:
            request.order.field = sort
            request.order.type = sort_ord  # type: ignore
        request.fields.extend(fields)

    return request


@query_parse_dependency_observer.wrap({"type": "convert_vectors"})
async def convert_vectors(kbid: str, query: str) -> list[utils_pb2.RelationNode]:
    predict = get_predict()
    return await predict.convert_sentence_to_vector(kbid, query)


@query_parse_dependency_observer.wrap({"type": "detect_entities"})
async def detect_entities(kbid: str, query: str) -> list[utils_pb2.RelationNode]:
    predict = get_predict()
    try:
        return await predict.detect_entities(kbid, query)
    except SendToPredictError as ex:
        logger.warning(f"Errors on predict api detecting entities: {ex}")
        return []


def expand_entities(
    meta_cache: EntitiesMetaCache,
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
            source_duplicate = duplicated_entities_by_value[entity.subtype][
                entity.value
            ]
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
        expanded_expression = {"and": [{"literal": entity} for entity in added_filters]}
        if request.filter.expression:
            expression = json.loads(request.filter.expression)
            expanded_expression["and"].extend(expression)
        request.filter.expression = json.dumps(expanded_expression)
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
) -> nodereader_pb2.SuggestRequest:
    request = nodereader_pb2.SuggestRequest()

    request.body = query
    if SuggestOptions.ENTITIES in features:
        request.features.append(nodereader_pb2.SuggestFeatures.ENTITIES)

    if SuggestOptions.PARAGRAPH in features:
        request.features.append(nodereader_pb2.SuggestFeatures.PARAGRAPHS)
        filters = [translate_label(fltr) for fltr in filters]
        request.filter.field_labels.extend(filters)
        request.fields.extend(fields)

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


@query_parse_dependency_observer.wrap({"type": "min_score"})
async def get_kb_model_default_min_score(kbid: str) -> Optional[float]:
    txn = await get_read_only_transaction()
    kbdm = KnowledgeBoxDataManager(get_driver(), read_only_txn=txn)
    model = await kbdm.get_model_metadata(kbid)
    if model.HasField("default_min_score"):
        return model.default_min_score
    else:
        return None


@alru_cache(maxsize=None)
async def get_default_semantic_min_score(kbid: str) -> float:
    fallback = 0.7
    model_min_score = await get_kb_model_default_min_score(kbid)
    if model_min_score is not None:
        return model_min_score
    return fallback


@query_parse_dependency_observer.wrap({"type": "synonyms"})
async def get_kb_synonyms(kbid: str) -> Optional[knowledgebox_pb2.Synonyms]:
    txn = await get_read_only_transaction()
    return await Synonyms(txn, kbid).get()


@query_parse_dependency_observer.wrap({"type": "entities_meta_cache"})
async def get_entities_meta_cache(kbid: str) -> EntitiesMetaCache:
    txn = await get_read_only_transaction()
    return await EntitiesDataManager.get_entities_meta_cache(kbid, txn)


@query_parse_dependency_observer.wrap({"type": "deleted_entities_groups"})
async def get_deleted_entity_groups(kbid: str) -> list[str]:
    txn = await get_read_only_transaction()
    return list(
        (await EntitiesDataManager.get_deleted_groups(kbid, txn)).entities_groups
    )


@query_parse_dependency_observer.wrap({"type": "classification_labels"})
async def get_classification_labels(kbid: str) -> knowledgebox_pb2.Labels:
    txn = await get_read_only_transaction()
    return await LabelsDataManager.inner_get_labels(kbid, txn)


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
        # Nested expressions are not allowed with paragraph labels
        if "literal" not in term:
            raise InvalidQueryError(
                "filters",
                "Paragraph labels can only be used with 'all' filter",
            )
