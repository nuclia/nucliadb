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


from nidx_protos.nodereader_pb2 import FilterExpression as PBFilterExpression
from typing_extensions import assert_never

from nucliadb.common import datamanagers
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.ids import FIELD_TYPE_NAME_TO_STR
from nucliadb_models.filters import (
    And,
    DateCreated,
    DateModified,
    FacetFilter,
    Field,
    FieldFilterExpression,
    Keyword,
    Not,
    Or,
    ParagraphFilterExpression,
    Resource,
)
from nucliadb_models.filters import facet_from_filter as models_facet_from_filter
from nucliadb_models.filters import filter_from_facet as models_filter_from_facet


async def parse_expression(
    expr: FieldFilterExpression | ParagraphFilterExpression,
    kbid: str,
) -> PBFilterExpression:
    f = PBFilterExpression()

    if isinstance(expr, And):
        for op in expr.operands:
            f.bool_and.operands.append(await parse_expression(op, kbid))
    elif isinstance(expr, Or):
        for op in expr.operands:
            f.bool_or.operands.append(await parse_expression(op, kbid))
    elif isinstance(expr, Not):
        f.bool_not.CopyFrom(await parse_expression(expr.operand, kbid))
    elif isinstance(expr, Resource):
        if expr.id:
            f.resource.resource_id = expr.id
        elif expr.slug:
            rid = await datamanagers.atomic.resources.get_resource_uuid_from_slug(
                kbid=kbid, slug=expr.slug
            )
            if rid is None:
                raise InvalidQueryError("slug", f"Cannot find slug {expr.slug}")
            f.resource.resource_id = rid
        else:  # pragma: no cover
            # Cannot happen due to model validation
            raise ValueError("Resource needs id or slug")
    elif isinstance(expr, Field):
        f.field.field_type = FIELD_TYPE_NAME_TO_STR[expr.type]
        if expr.name:
            f.field.field_id = expr.name
    elif isinstance(expr, Keyword):
        f.keyword.keyword = expr.word
    elif isinstance(expr, DateCreated):
        f.date.field = PBFilterExpression.DateRangeFilter.DateField.CREATED
        if expr.since:
            f.date.since.FromDatetime(expr.since)
        if expr.until:
            f.date.until.FromDatetime(expr.until)
    elif isinstance(expr, DateModified):
        f.date.field = PBFilterExpression.DateRangeFilter.DateField.MODIFIED
        if expr.since:
            f.date.since.FromDatetime(expr.since)
        if expr.until:
            f.date.until.FromDatetime(expr.until)
    elif isinstance(expr, FacetFilter):
        f.facet.facet = facet_from_filter(expr)
    else:
        assert_never(expr)

    return f


def facet_from_filter(expr: FacetFilter) -> str:
    try:
        return models_facet_from_filter(expr)
    except ValueError as err:
        raise InvalidQueryError("filters", str(err))


def filter_from_facet(facet: str) -> FacetFilter:
    try:
        return models_filter_from_facet(facet)
    except ValueError as err:
        raise InvalidQueryError("filters", str(err))


def add_and_expression(dest: PBFilterExpression, add: PBFilterExpression):
    dest_expr_type = dest.WhichOneof("expr")
    if dest_expr_type is None:
        dest.CopyFrom(add)
    elif dest_expr_type == "bool_and":
        dest.bool_and.operands.append(add)
    else:
        and_expr = PBFilterExpression()
        and_expr.bool_and.operands.append(dest)
        and_expr.bool_and.operands.append(add)
        dest.CopyFrom(and_expr)
