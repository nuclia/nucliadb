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

from nucliadb.common import datamanagers
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.filters import translate_label
from nucliadb.search.search.query_parser.filter_expression import FacetFilterTypes, facet_from_filter
from nucliadb.search.search.query_parser.models import (
    CatalogExpression,
    CatalogQuery,
)
from nucliadb_models import search as search_models
from nucliadb_models.filters import (
    And,
    DateCreated,
    DateModified,
    Not,
    Or,
    Resource,
    ResourceFilterExpression,
)
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    SortField,
    SortOptions,
    SortOrder,
)


async def parse_catalog(kbid: str, item: search_models.CatalogRequest) -> CatalogQuery:
    has_old_filters = (
        item.filters
        or item.range_creation_start is not None
        or item.range_creation_end is not None
        or item.range_modification_start is not None
        or item.range_modification_end is not None
        or item.with_status is not None
    )
    if item.filter_expression is not None and has_old_filters:
        raise InvalidQueryError("filter_expression", "Cannot mix old filters with filter_expression")

    if has_old_filters:
        catalog_expr = parse_old_filters(item)
    elif item.filter_expression:
        catalog_expr = await parse_filter_expression(item.filter_expression.resource, kbid)
    else:
        catalog_expr = None

    if item.hidden is not None:
        if item.hidden:
            hidden_filter = CatalogExpression(facet=LABEL_HIDDEN)
        else:
            hidden_filter = CatalogExpression(bool_not=CatalogExpression(facet=LABEL_HIDDEN))

        if catalog_expr:
            catalog_expr = CatalogExpression(bool_and=[catalog_expr, hidden_filter])
        else:
            catalog_expr = hidden_filter

    sort = item.sort
    if sort is None:
        # By default we sort by creation date (most recent first)
        sort = SortOptions(
            field=SortField.CREATED,
            order=SortOrder.DESC,
            limit=None,
        )

    return CatalogQuery(
        kbid=kbid,
        query=item.query,
        filters=catalog_expr,
        sort=sort,
        faceted=item.faceted,
        page_number=item.page_number,
        page_size=item.page_size,
    )


def parse_old_filters(item: search_models.CatalogRequest) -> CatalogExpression:
    expressions = []

    for fltr in item.filters or []:
        if isinstance(fltr, str):
            expressions.append(CatalogExpression(facet=translate_label(fltr)))
        elif fltr.all:
            filters = [CatalogExpression(facet=translate_label(f)) for f in fltr.all]
            expressions.append(CatalogExpression(bool_and=filters))
        elif fltr.any:
            filters = [CatalogExpression(facet=translate_label(f)) for f in fltr.any]
            expressions.append(CatalogExpression(bool_or=filters))
        elif fltr.none:
            filters = [CatalogExpression(facet=translate_label(f)) for f in fltr.none]
            expressions.append(CatalogExpression(bool_not=CatalogExpression(bool_or=filters)))
        elif fltr.not_all:
            filters = [CatalogExpression(facet=translate_label(f)) for f in fltr.not_all]
            expressions.append(CatalogExpression(bool_not=CatalogExpression(bool_and=filters)))

    if item.range_creation_start or item.range_creation_end:
        expressions.append(
            CatalogExpression(
                date=CatalogExpression.Date(
                    field="created_at",
                    since=item.range_creation_start,
                    until=item.range_creation_end,
                )
            )
        )

    if item.range_modification_start or item.range_modification_end:
        expressions.append(
            CatalogExpression(
                date=CatalogExpression.Date(
                    field="modified_at",
                    since=item.range_modification_start,
                    until=item.range_modification_end,
                )
            )
        )

    if item.with_status:
        if item.with_status == ResourceProcessingStatus.PROCESSED:
            expressions.append(
                CatalogExpression(
                    bool_or=[
                        CatalogExpression(facet="/n/s/PROCESSED"),
                        CatalogExpression(facet="/n/s/ERROR"),
                    ]
                )
            )
        else:
            expressions.append(CatalogExpression(facet="/n/s/PENDING"))

    return CatalogExpression(bool_and=expressions)


async def parse_filter_expression(expr: ResourceFilterExpression, kbid: str) -> CatalogExpression:
    cat = CatalogExpression()
    if isinstance(expr, And):
        cat.bool_and = []
        for op in expr.operands:
            cat.bool_and.append(await parse_filter_expression(op, kbid))
    elif isinstance(expr, Or):
        cat.bool_or = []
        for op in expr.operands:
            cat.bool_or.append(await parse_filter_expression(op, kbid))
    elif isinstance(expr, Not):
        cat.bool_not = await parse_filter_expression(expr.operand, kbid)
    elif isinstance(expr, Resource):
        if expr.id:
            cat.resource_id = expr.id
        elif expr.slug:
            rid = await datamanagers.atomic.resources.get_resource_uuid_from_slug(
                kbid=kbid, slug=expr.slug
            )
            if rid is None:
                raise InvalidQueryError("slug", f"Cannot find slug {expr.slug}")
            cat.resource_id = rid
        else:  # pragma: nocover
            # Cannot happen due to model validation
            raise ValueError("Resource needs id or slug")
    elif isinstance(expr, DateCreated):
        cat.date = CatalogExpression.Date(field="created_at", since=expr.since, until=expr.until)
    elif isinstance(expr, DateModified):
        cat.date = CatalogExpression.Date(field="modified_at", since=expr.since, until=expr.until)
    elif isinstance(expr, FacetFilterTypes):
        cat.facet = facet_from_filter(expr)
    else:
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return cat
