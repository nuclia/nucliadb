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

from typing import Union

from nucliadb.common import datamanagers
from nucliadb.common.ids import FIELD_TYPE_NAME_TO_STR
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb_models.filter import (
    And,
    DateCreated,
    DateModified,
    Entity,
    Field,
    FieldFilterExpression,
    FieldMimetype,
    Generated,
    Keyword,
    Kind,
    Label,
    Language,
    Not,
    Or,
    OriginMetadata,
    OriginPath,
    OriginTag,
    ParagraphFilterExpression,
    Resource,
    ResourceMimetype,
)
from nucliadb_protos.nodereader_pb2 import FilterExpression as PBFilterExpression


async def parse_expression(
    expr: Union[FieldFilterExpression, ParagraphFilterExpression],
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
        else:
            rid = await datamanagers.atomic.resources.get_resource_uuid_from_slug(
                kbid=kbid, slug=expr.slug
            )
            if rid is None:
                raise InvalidQueryError("slug", f"Cannot find slug {expr.slug}")
            f.resource.resource_id = rid
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
    elif isinstance(expr, OriginTag):
        f.facet.facet = f"/t/{expr.tag}"
    elif isinstance(expr, Label):
        f.facet.facet = f"/l/{expr.labelset}"
        if expr.label:
            f.facet.facet += f"/{expr.label}"
    elif isinstance(expr, ResourceMimetype):
        f.facet.facet = f"/n/i/{expr.type}"
        if expr.subtype:
            f.facet.facet += f"/{expr.subtype}"
    elif isinstance(expr, FieldMimetype):
        f.facet.facet = f"/mt/{expr.type}"
        if expr.subtype:
            f.facet.facet += f"/{expr.subtype}"
    elif isinstance(expr, Entity):
        f.facet.facet = f"/e/{expr.subtype}"
        if expr.value:
            f.facet.facet += f"/{expr.value}"
    elif isinstance(expr, Language):
        if expr.only_primary:
            f.facet.facet = f"/s/p/{expr.language}"
        else:
            f.facet.facet = f"/s/s/{expr.language}"
    elif isinstance(expr, OriginMetadata):
        f.facet.facet = f"/m/{expr.field}"
        if expr.value:
            f.facet.facet += f"/{expr.value}"
    elif isinstance(expr, OriginPath):
        f.facet.facet = f"/p/{expr.prefix}"
    elif isinstance(expr, Generated):
        f.facet.facet = "/g/da"
        if expr.da_task:
            f.facet.facet += f"/{expr.da_task}"
    elif isinstance(expr, Kind):
        f.facet.facet = f"/k/{expr.kind.lower()}"
    else:
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return f


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
