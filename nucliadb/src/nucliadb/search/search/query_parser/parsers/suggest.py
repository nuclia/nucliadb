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

from nidx_protos import nodereader_pb2

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.filter_expression import add_and_expression, parse_expression
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.old_filters import OldFilterParams, parse_old_filters
from nucliadb.search.search.utils import filter_hidden_resources, kb_security_enforced
from nucliadb_models.filters import FilterExpression
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_models.search import (
    SuggestOptions,
)
from nucliadb_models.security import RequestSecurity


@query_parser_observer.wrap({"type": "parse_suggest"})
async def parse_suggest(
    kbid: str,
    query: str,
    features: list[SuggestOptions],
    filter_expression: FilterExpression | None,
    fields: list[str],
    filters: list[str],
    show_hidden: bool,
    range_creation_start: datetime | None = None,
    range_creation_end: datetime | None = None,
    range_modification_start: datetime | None = None,
    range_modification_end: datetime | None = None,
    security_groups: list[str] | None = None,
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

    security = await kb_security_enforced(
        kbid, RequestSecurity(groups=security_groups) if security_groups is not None else None
    )
    if security is not None:
        request.security.access_groups.extend(security.groups)

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

    hidden = await filter_hidden_resources(kbid, show_hidden)
    if hidden is not None:
        expr = nodereader_pb2.FilterExpression()
        if hidden:
            expr.facet.facet = LABEL_HIDDEN
        else:
            expr.bool_not.facet.facet = LABEL_HIDDEN

        add_and_expression(request.field_filter, expr)

    return request
