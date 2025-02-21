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


def parse_filter_expression(
    expr: Union[FieldFilterExpression, ParagraphFilterExpression],
) -> PBFilterExpression:
    f = PBFilterExpression()

    if isinstance(expr, And):
        f.bool_and.operands.extend((parse_filter_expression(e) for e in expr.operands))
    elif isinstance(expr, Or):
        f.bool_or.operands.extend((parse_filter_expression(e) for e in expr.operands))
    elif isinstance(expr, Not):
        f.bool_not.CopyFrom(parse_filter_expression(expr.operand))
    elif isinstance(expr, Resource):
        if expr.id:
            f.resource.resource_id = expr.id
        else:
            # TODO: Slug
            raise Exception("Slug not implemented")
    elif isinstance(expr, Field):
        f.field.field_type = expr.type
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
        f.facet.facet = f"/t/{expr.labelset}"
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
