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

import pytest

from nucliadb.common.catalog.dummy import DummyCatalog
from nucliadb.common.catalog.interface import CatalogQuery, CatalogResourceData
from nucliadb_models import search as search_models
from nucliadb_models.search import CatalogFacetsRequest, SortField, SortOptions


@pytest.mark.asyncio
async def test_dummy_catalog():
    catalog = DummyCatalog()

    await catalog.update(
        None,
        "kbid",
        "rid",
        CatalogResourceData(
            title="Test Title",
            created_at="2021-01-01T00:00:00Z",
            modified_at="2021-01-01T00:00:00Z",
            labels=["/l/test"],
            slug="test-title",
        ),
    )

    await catalog.delete(None, "kbid", "rid")

    results = await catalog.search(
        CatalogQuery(
            kbid="kbid",
            query=search_models.CatalogQuery(
                query="foo",
            ),
            filters=None,
            sort=SortOptions(field=SortField.TITLE, limit=10),
            faceted=[],
            page_size=10,
            page_number=0,
        )
    )
    assert results.results == []

    facets = await catalog.facets("kbid", CatalogFacetsRequest(prefixes=[]))
    assert facets == {}
