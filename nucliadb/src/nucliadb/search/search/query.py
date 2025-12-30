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
from typing import Any

from nidx_protos import nodereader_pb2
from nidx_protos.noderesources_pb2 import Resource

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import add_and_expression, parse_expression
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb_models.filters import FilterExpression
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    SortField,
    SortOrder,
    SuggestOptions,
)

from .query_parser.old_filters import OldFilterParams, parse_old_filters


async def paragraph_query_to_pb(
    kbid: str,
    rid: str,
    query: str,
    filter_expression: FilterExpression | None,
    fields: list[str],
    filters: list[str],
    faceted: list[str],
    top_k: int,
    range_creation_start: datetime | None = None,
    range_creation_end: datetime | None = None,
    range_modification_start: datetime | None = None,
    range_modification_end: datetime | None = None,
    sort: str | None = None,
    sort_ord: str = SortOrder.DESC.value,
    with_duplicates: bool = False,
) -> nodereader_pb2.SearchRequest:
    request = nodereader_pb2.SearchRequest()
    request.paragraph = True

    # We need to ask for all and cut later
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
        query_image=None,
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


async def suggest_query_to_pb(
    kbid: str,
    features: list[SuggestOptions],
    query: str,
    filter_expression: FilterExpression | None,
    fields: list[str],
    filters: list[str],
    faceted: list[str],
    range_creation_start: datetime | None = None,
    range_creation_end: datetime | None = None,
    range_modification_start: datetime | None = None,
    range_modification_end: datetime | None = None,
    hidden: bool | None = None,
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
        query_image=None,
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


def get_sort_field_proto(obj: SortField) -> nodereader_pb2.OrderBy.OrderField.ValueType | None:
    return {
        SortField.SCORE: None,
        SortField.CREATED: nodereader_pb2.OrderBy.OrderField.CREATED,
        SortField.MODIFIED: nodereader_pb2.OrderBy.OrderField.MODIFIED,
        SortField.TITLE: None,
    }[obj]
