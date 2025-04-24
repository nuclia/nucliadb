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
from typing import Any, Optional

from nidx_protos import nodereader_pb2
from nidx_protos.noderesources_pb2 import Resource

from nucliadb.common import datamanagers
from nucliadb.search.search.filters import (
    translate_label,
)
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb_models.filters import FilterExpression
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    SortField,
    SortOrder,
    SuggestOptions,
)
from nucliadb_protos import utils_pb2

from .exceptions import InvalidQueryError
from .query_parser.filter_expression import add_and_expression, parse_expression
from .query_parser.old_filters import OldFilterParams, parse_old_filters


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
