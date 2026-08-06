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

from nucliadb.common.filter_expression import add_and_expression
from nucliadb.search.search.metrics import query_parser_observer
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.parsers.common import parse_filters
from nucliadb_models.filters import FilterExpression
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_models.search import (
    SuggestOptions,
)
from nucliadb_models.security import RequestSecurity

# This is a quite arbitrary number we set loong time ago. If needed, we
# could allow users to set it
MAX_SUGGEST_RESULTS = 10


@query_parser_observer.wrap({"type": "parse_suggest"})
async def parse_suggest(
    kbid: str,
    query: str,
    features: list[SuggestOptions],
    filter_expression: FilterExpression | None,
    fields: list[str],
    label_filters: list[str],
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

    request.top_k = MAX_SUGGEST_RESULTS

    fetcher = Fetcher(
        kbid,
        query=query,
        user_vector=None,
        vectorset=None,
        rephrase=False,
        rephrase_prompt=None,
        generative_model=None,
        query_image=None,
    )

    filters = await parse_filters(
        kbid,
        fetcher,
        show_hidden=show_hidden,
        security=RequestSecurity(groups=security_groups) if security_groups is not None else None,
        with_duplicates=False,  # unused
        filter_expression=filter_expression,
        label_filters=label_filters,
        keyword_filters=None,
        resource_filters=None,
        fields=fields,
        range_creation_start=range_creation_start,
        range_creation_end=range_creation_end,
        range_modification_start=range_modification_start,
        range_modification_end=range_modification_end,
    )

    if filters.security is not None:
        request.security.access_groups.extend(filters.security.groups)

    if filters.field_expression:
        request.field_filter.CopyFrom(filters.field_expression)
    if filters.paragraph_expression:
        request.paragraph_filter.CopyFrom(filters.paragraph_expression)
    if filters.json_expression is not None:
        request.json_filter.CopyFrom(filters.json_expression)
    request.filter_operator = filters.filter_expression_operator

    if filters.hidden is not None:
        expr = nodereader_pb2.FilterExpression()
        if filters.hidden:
            expr.facet.facet = LABEL_HIDDEN
        else:
            expr.bool_not.facet.facet = LABEL_HIDDEN

        add_and_expression(request.field_filter, expr)

    return request
