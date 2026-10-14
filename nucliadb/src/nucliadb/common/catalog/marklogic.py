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

import logging
import re

from nucliadb.common.catalog.interface import (
    Catalog,
    CatalogQuery,
)
from nucliadb_models.search import (
    CatalogFacetsRequest,
    ResourceResult,
    Resources,
)
from nucliadb_telemetry import metrics

search_observer = metrics.Observer("marklogic_catalog_search", labels={"op": ""})

logger = logging.getLogger(__name__)

SPLIT_REGEX = re.compile(r"\W")


class MarkLogicCatalog(Catalog):
    @search_observer.wrap({"op": "search"})
    async def search(self, catalog_query: CatalogQuery) -> Resources:
        # TODO(Marklogic)
        return Resources(
            facets=None,
            results=[
                ResourceResult(
                    rid=str(r["rid"]).replace("-", ""),
                    field="title",
                    field_type="a",
                    labels=[label for label in r["labels"] if label.startswith("/l/")],
                    score=0,
                )
                for r in []
            ],
            query=catalog_query.query.query if catalog_query.query else "",
            total=0,
            page_number=catalog_query.page_number,
            page_size=catalog_query.page_size,
            next_page=(catalog_query.page_size * catalog_query.page_number + len([r for r in []]) < 0),
            min_score=0,
        )

    @search_observer.wrap({"op": "catalog_facets"})
    async def facets(self, kbid: str, request: CatalogFacetsRequest) -> dict[str, int]:
        # TODO(Marklogic)
        return {}
