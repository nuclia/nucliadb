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
from datetime import datetime
from typing import List, Optional, Tuple

from async_lru import alru_cache
from fastapi import HTTPException
from nucliadb_protos.nodereader_pb2 import (
    ParagraphSearchRequest,
    SearchRequest,
    SuggestFeatures,
    SuggestRequest,
)
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb.common.datamanagers.kb import KnowledgeBoxDataManager
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search import logger
from nucliadb.search.predict import PredictVectorMissing, SendToPredictError
from nucliadb.search.search.metrics import node_features
from nucliadb.search.search.synonyms import apply_synonyms_to_request
from nucliadb.search.utilities import get_predict
from nucliadb_models.labels import (
    translate_alias_to_system_label,
    translate_system_to_alias_label,
)
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    SearchOptions,
    SortFieldMap,
    SortOptions,
    SortOrder,
    SortOrderMap,
    SuggestOptions,
)
from nucliadb_telemetry.metrics import Counter
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

from .exceptions import InvalidQueryError

ENTITY_FILTER_PREFIX = "/e/"
LABEL_FILTER_PREFIX = "/l/"


def translate_label_filters(filters: List[str]) -> List[str]:
    """
    Translate friendly filter names to the shortened filter names.
    """
    output = []
    for fltr in filters:
        if len(fltr) == 0:
            raise InvalidQueryError("filters", f"Invalid empty label")
        if fltr[0] != "/":
            raise InvalidQueryError(
                "filters", f"Invalid label. It must start with a `/`: {fltr}"
            )

        output.append(translate_alias_to_system_label(fltr))
    return output


def record_filters_counter(filters: list[str], counter: Counter) -> None:
    counter.inc({"type": "filters"})
    filters.sort()
    entity_found = False
    label_found = False
    for fltr in filters:
        if entity_found and label_found:
            break
        if not entity_found and fltr.startswith(ENTITY_FILTER_PREFIX):
            entity_found = True
            counter.inc({"type": "filters_entities"})
        elif not label_found and fltr.startswith(LABEL_FILTER_PREFIX):
            label_found = True
            counter.inc({"type": "filters_labels"})


async def global_query_to_pb(
    kbid: str,
    features: List[SearchOptions],
    query: str,
    filters: List[str],
    faceted: List[str],
    page_number: int,
    page_size: int,
    min_score: float,
    sort: Optional[SortOptions],
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    fields: Optional[List[str]] = None,
    user_vector: Optional[List[float]] = None,
    vectorset: Optional[str] = None,
    with_duplicates: bool = False,
    with_status: Optional[ResourceProcessingStatus] = None,
    with_synonyms: bool = False,
    autofilter: bool = False,
    key_filters: Optional[List[str]] = None,
) -> Tuple[SearchRequest, bool, List[str]]:
    """
    Converts the pydantic query to a protobuf query

    :return: (request, incomplete, autofilters)
        where:
            - request: protobuf SearchRequest object
            - incomplete: If the query is incomplete (missing vectors)
            - autofilters: The autofilters that were applied
    """
    fields = fields or []
    autofilters = []

    request = SearchRequest()
    request.min_score = min_score
    request.body = query
    request.with_duplicates = with_duplicates
    if len(filters) > 0:
        filters = translate_label_filters(filters)
        request.filter.tags.extend(filters)
        record_filters_counter(filters, node_features)

    request.faceted.tags.extend(translate_label_filters(faceted))
    request.fields.extend(fields)

    if key_filters is not None and len(key_filters) > 0:
        request.key_filters.extend(key_filters)
        node_features.inc({"type": "key_filters"})

    if with_status is not None:
        request.with_status = PROCESSING_STATUS_TO_PB_MAP[with_status]

    # We need to ask for all and cut later
    request.page_number = 0
    if sort and sort.limit is not None:
        # As the index can't sort, we have to do it when merging. To
        # have consistent results, we must limit them
        request.result_per_page = sort.limit
    else:
        request.result_per_page = page_number * page_size + page_size

    if range_creation_start is not None:
        request.timestamps.from_created.FromDatetime(range_creation_start)

    if range_creation_end is not None:
        request.timestamps.to_created.FromDatetime(range_creation_end)

    if range_modification_start is not None:
        request.timestamps.from_modified.FromDatetime(range_modification_start)

    if range_modification_end is not None:
        request.timestamps.to_modified.FromDatetime(range_modification_end)

    document_search = SearchOptions.DOCUMENT in features
    paragraph_search = SearchOptions.PARAGRAPH in features
    if document_search or paragraph_search:
        sort_field = SortFieldMap[sort.field] if sort else None
        if sort_field is not None:
            request.order.sort_by = sort_field
            request.order.type = SortOrderMap[sort.order]  # type: ignore

    if document_search:
        request.document = True
        node_features.inc({"type": "documents"})

    if paragraph_search:
        request.paragraph = True
        node_features.inc({"type": "paragraphs"})

    incomplete = False
    vector_search = SearchOptions.VECTOR in features
    if vector_search:
        node_features.inc({"type": "vectors"})
        if vectorset is not None:
            node_features.inc({"type": "vectorset"})
        incomplete = await _parse_vectors(
            request, kbid, query, user_vector=user_vector, vectorset=vectorset
        )

    relations_search = SearchOptions.RELATIONS in features
    if relations_search or autofilter:
        detected_entities = await detect_entities(kbid, query)
        if relations_search:
            request.relation_subgraph.entry_points.extend(detected_entities)
            request.relation_subgraph.depth = 1
            node_features.inc({"type": "relations"})
        if autofilter:
            entity_filters = parse_entities_to_filters(request, detected_entities)
            autofilters.extend(
                [translate_system_to_alias_label(e) for e in entity_filters]
            )

    if with_synonyms:
        if vector_search or relations_search:
            raise HTTPException(
                status_code=422,
                detail="Search with custom synonyms is only supported on paragraph and document search",
            )
        await apply_synonyms_to_request(request, kbid)

    return request, incomplete, autofilters


async def _parse_vectors(
    request: SearchRequest,
    kbid: str,
    query: str,
    user_vector: Optional[List[float]],
    vectorset: Optional[str],
) -> bool:
    incomplete = False
    if vectorset is not None:
        request.vectorset = vectorset
    if user_vector is None:
        predict = get_predict()
        try:
            predict_vector = await predict.convert_sentence_to_vector(kbid, query)
            request.vector.extend(predict_vector)
        except SendToPredictError as err:
            logger.warning(f"Errors on predict api trying to embedd query: {err}")
            incomplete = True
        except PredictVectorMissing:
            logger.warning("Predict api returned an empty vector")
            incomplete = True
    else:
        request.vector.extend(user_vector)
    return incomplete


async def detect_entities(kbid: str, query: str) -> List[RelationNode]:
    predict = get_predict()
    try:
        return await predict.detect_entities(kbid, query)
    except SendToPredictError as ex:
        logger.warning(f"Errors on predict api detecting entities: {ex}")
        return []


def parse_entities_to_filters(
    request: SearchRequest, detected_entities: List[RelationNode]
) -> List[str]:
    added_filters = []
    for entity_filter in [
        f"/e/{entity.subtype}/{entity.value}"
        for entity in detected_entities
        if entity.ntype == RelationNode.NodeType.ENTITY
    ]:
        if entity_filter not in request.filter.tags:
            request.filter.tags.append(entity_filter)
            added_filters.append(entity_filter)
    return added_filters


def suggest_query_to_pb(
    features: List[SuggestOptions],
    query: str,
    fields: List[str],
    filters: List[str],
    faceted: List[str],
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
) -> SuggestRequest:
    request = SuggestRequest()

    request.body = query
    if SuggestOptions.ENTITIES in features:
        request.features.append(SuggestFeatures.ENTITIES)

    if SuggestOptions.PARAGRAPH in features:
        request.features.append(SuggestFeatures.PARAGRAPHS)
        filters = translate_label_filters(filters)
        request.filter.tags.extend(filters)
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


async def paragraph_query_to_pb(
    features: List[SearchOptions],
    rid: str,
    query: str,
    fields: List[str],
    filters: List[str],
    faceted: List[str],
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    sort: Optional[str] = None,
    sort_ord: str = SortOrder.DESC.value,
    with_duplicates: bool = False,
) -> ParagraphSearchRequest:
    request = ParagraphSearchRequest()
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
        request.filter.tags.extend(translate_label_filters(filters))
        request.faceted.tags.extend(translate_label_filters(faceted))
        if sort:
            request.order.field = sort
            request.order.type = sort_ord  # type: ignore
        request.fields.extend(fields)

    return request


PROCESSING_STATUS_TO_PB_MAP = {
    ResourceProcessingStatus.PENDING: Resource.ResourceStatus.PENDING,
    ResourceProcessingStatus.PROCESSED: Resource.ResourceStatus.PROCESSED,
    ResourceProcessingStatus.ERROR: Resource.ResourceStatus.ERROR,
    ResourceProcessingStatus.EMPTY: Resource.ResourceStatus.EMPTY,
    ResourceProcessingStatus.BLOCKED: Resource.ResourceStatus.BLOCKED,
    ResourceProcessingStatus.EXPIRED: Resource.ResourceStatus.EXPIRED,
}


async def get_kb_model_default_min_score(kbid: str) -> Optional[float]:
    driver = get_driver()
    kbdm = KnowledgeBoxDataManager(driver)
    model = await kbdm.get_model_metadata(kbid)
    if model.HasField("default_min_score"):
        return model.default_min_score
    else:
        return None


@alru_cache(maxsize=None)
async def get_default_min_score(kbid: str) -> float:
    fallback = 0.7
    if not has_feature(const.Features.DEFAULT_MIN_SCORE):
        return fallback

    model_min_score = await get_kb_model_default_min_score(kbid)
    if model_min_score is not None:
        return model_min_score
    else:
        # B/w compatible code until we figure out how to
        # set default min score for old on-prem kbs
        return fallback
