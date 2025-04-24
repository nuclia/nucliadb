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

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

from nidx_protos.nodereader_pb2 import FilterExpression

from nucliadb.search.search.filters import translate_label
from nucliadb_models.search import (
    Filter,
)
from nucliadb_protos import knowledgebox_pb2

from .exceptions import InvalidQueryError
from .fetcher import Fetcher


@dataclass
class OldFilterParams:
    label_filters: Union[list[str], list[Filter]]
    keyword_filters: Union[list[str], list[Filter]]
    range_creation_start: Optional[datetime] = None
    range_creation_end: Optional[datetime] = None
    range_modification_start: Optional[datetime] = None
    range_modification_end: Optional[datetime] = None
    fields: Optional[list[str]] = None
    key_filters: Optional[list[str]] = None


async def parse_old_filters(
    old: OldFilterParams, fetcher: Fetcher
) -> tuple[Optional[FilterExpression], Optional[FilterExpression]]:
    filters = []
    paragraph_filter_expression = None

    # Labels
    if old.label_filters:
        classification_labels = await fetcher.get_classification_labels()

        paragraph_exprs = []
        for fltr in old.label_filters:
            field_expr, paragraph_expr = convert_label_filter_to_expressions(fltr, classification_labels)
            if field_expr:
                filters.append(field_expr)
            if paragraph_expr:
                paragraph_exprs.append(paragraph_expr)

        if len(paragraph_exprs) == 1:
            paragraph_filter_expression = paragraph_exprs[0]
        elif len(paragraph_exprs) > 1:
            paragraph_filter_expression = FilterExpression()
            paragraph_filter_expression.bool_and.operands.extend(paragraph_exprs)

    # Keywords
    if old.keyword_filters:
        for fltr in old.keyword_filters:
            filters.append(convert_keyword_filter_to_expression(fltr))

    # Timestamps
    if old.range_creation_start is not None or old.range_creation_end is not None:
        f = FilterExpression()
        f.date.field = FilterExpression.DateRangeFilter.DateField.CREATED
        if old.range_creation_start is not None:
            f.date.since.FromDatetime(old.range_creation_start)
        if old.range_creation_end is not None:
            f.date.until.FromDatetime(old.range_creation_end)
        filters.append(f)

    if old.range_modification_start is not None or old.range_modification_end is not None:
        f = FilterExpression()
        f.date.field = FilterExpression.DateRangeFilter.DateField.MODIFIED
        if old.range_modification_start is not None:
            f.date.since.FromDatetime(old.range_modification_start)
        if old.range_modification_end is not None:
            f.date.until.FromDatetime(old.range_modification_end)
        filters.append(f)

    # Fields
    if old.fields:
        field_filters = []
        for field in old.fields:
            parts = field.split("/")
            f = FilterExpression()
            f.field.field_type = parts[0]
            if len(parts) > 1:
                f.field.field_id = parts[1]
            field_filters.append(f)

        if len(field_filters) > 1:
            f = FilterExpression()
            f.bool_or.operands.extend(field_filters)
            filters.append(f)
        else:
            filters.append(field_filters[0])

    # Key filter
    if old.key_filters:
        key_exprs = []
        for key in old.key_filters:
            expr = FilterExpression()
            parts = key.split("/")
            if len(parts) == 1:
                expr.resource.resource_id = parts[0]
            else:
                r = FilterExpression()
                r.resource.resource_id = parts[0]
                expr.bool_and.operands.append(r)
                f = FilterExpression()
                f.field.field_type = parts[1]
                if len(parts) > 2:
                    f.field.field_id = parts[2]
            key_exprs.append(expr)

        if len(key_exprs) == 1:
            filters.append(key_exprs[0])
        elif len(key_exprs) > 1:
            f = FilterExpression()
            f.bool_or.operands.extend(key_exprs)
            filters.append(f)

    # Build filter
    if len(filters) == 0:
        return None, paragraph_filter_expression
    elif len(filters) == 1:
        return filters[0], paragraph_filter_expression
    else:
        f = FilterExpression()
        f.bool_and.operands.extend(filters)
        return f, paragraph_filter_expression


def convert_label_filter_to_expressions(
    fltr: Union[str, Filter], classification_labels: knowledgebox_pb2.Labels
) -> tuple[Optional[FilterExpression], Optional[FilterExpression]]:
    if isinstance(fltr, str):
        fltr = translate_label(fltr)
        f = FilterExpression()
        f.facet.facet = fltr
        if is_paragraph_label(fltr, classification_labels):
            return None, f
        else:
            return f, None

    if fltr.all:
        return split_labels(fltr.all, classification_labels, "bool_and", negate=False)
    if fltr.any:
        return split_labels(fltr.any, classification_labels, "bool_or", negate=False)
    if fltr.none:
        return split_labels(fltr.none, classification_labels, "bool_and", negate=True)
    if fltr.not_all:
        return split_labels(fltr.not_all, classification_labels, "bool_or", negate=True)

    return None, None


def split_labels(
    labels: list[str], classification_labels: knowledgebox_pb2.Labels, combinator: str, negate: bool
) -> tuple[Optional[FilterExpression], Optional[FilterExpression]]:
    field = []
    paragraph = []
    for label in labels:
        label = translate_label(label)
        expr = FilterExpression()
        if negate:
            expr.bool_not.facet.facet = label
        else:
            expr.facet.facet = label

        if is_paragraph_label(label, classification_labels):
            paragraph.append(expr)
        else:
            field.append(expr)

    if len(field) == 0:
        field_expr = None
    elif len(field) == 1:
        field_expr = field[0]
    else:
        field_expr = FilterExpression()
        filter_list = getattr(field_expr, combinator)
        filter_list.operands.extend(field)

    if len(paragraph) > 0 and combinator == "bool_or":
        raise InvalidQueryError(
            "filters",
            "Paragraph labels can only be used with 'all' filter",
        )

    if len(paragraph) == 0:
        paragraph_expr = None
    elif len(paragraph) == 1:
        paragraph_expr = paragraph[0]
    else:
        paragraph_expr = FilterExpression()
        filter_list = getattr(paragraph_expr, combinator)
        filter_list.extend(paragraph)

    return field_expr, paragraph_expr


def is_paragraph_label(label: str, classification_labels: knowledgebox_pb2.Labels) -> bool:
    if len(label) == 0 or label[0] != "/":
        return False
    if not label.startswith("/l/"):
        return False
    # Classification labels should have the form /l/labelset/label
    parts = label.split("/")
    if len(parts) < 4:
        return False
    labelset_id = parts[2]

    try:
        labelset: Optional[knowledgebox_pb2.LabelSet] = classification_labels.labelset.get(labelset_id)
        if labelset is None:
            return False
        return knowledgebox_pb2.LabelSet.LabelSetKind.PARAGRAPHS in labelset.kind
    except KeyError:
        # labelset_id not found
        return False


def convert_keyword_filter_to_expression(fltr: Union[str, Filter]) -> FilterExpression:
    if isinstance(fltr, str):
        return convert_keyword_to_expression(fltr)

    f = FilterExpression()
    if fltr.all:
        f.bool_and.operands.extend((convert_keyword_to_expression(f) for f in fltr.all))
    if fltr.any:
        f.bool_or.operands.extend((convert_keyword_to_expression(f) for f in fltr.any))
    if fltr.none:
        f.bool_not.bool_or.operands.extend((convert_keyword_to_expression(f) for f in fltr.none))
    if fltr.not_all:
        f.bool_not.bool_and.operands.extend((convert_keyword_to_expression(f) for f in fltr.not_all))

    return f


def convert_keyword_to_expression(keyword: str) -> FilterExpression:
    f = FilterExpression()
    f.keyword.keyword = keyword
    return f
