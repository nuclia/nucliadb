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
from __future__ import annotations

import abc
import datetime
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, Field

from nucliadb.common.maindb.driver import Transaction
from nucliadb_models import search as search_models
from nucliadb_models.search import CatalogFacetsRequest, Resources


class CatalogResourceData(BaseModel):
    """
    Data extracted from a resource to be indexed in the catalog
    """

    title: str = Field(description="Resource title")
    created_at: datetime.datetime = Field(description="Resource creation date")
    modified_at: datetime.datetime = Field(description="Resource last modification date")
    labels: list[str] = Field(
        description="Resource labels. This includes labels at the resource level and all classification labels of its fields"
    )
    slug: str = Field(description="Resource slug")


@dataclass
class CatalogExpression:
    @dataclass
    class Date:
        field: Literal["created_at"] | Literal["modified_at"]
        since: datetime.datetime | None
        until: datetime.datetime | None

    bool_and: list[CatalogExpression] | None = None
    bool_or: list[CatalogExpression] | None = None
    bool_not: CatalogExpression | None = None
    date: Date | None = None
    facet: str | None = None
    resource_id: str | None = None


class CatalogQuery(BaseModel):
    kbid: str
    query: search_models.CatalogQuery | None = Field(description="Full-text search query")
    filters: CatalogExpression | None = Field(description="Filters to apply to the search")
    sort: search_models.SortOptions = Field(description="Sorting option")
    faceted: list[str] = Field(description="List of facets to compute during the search")
    page_size: int = Field(description="Used for pagination. Maximum page size is 100")
    page_number: int = Field(description="Used for pagination. First page is 0")


class Catalog(abc.ABC, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def update(self, txn: Transaction, kbid: str, rid: str, data: CatalogResourceData): ...

    @abc.abstractmethod
    async def delete(self, txn: Transaction, kbid: str, rid: str): ...

    @abc.abstractmethod
    async def search(self, query: CatalogQuery) -> Resources: ...

    @abc.abstractmethod
    async def facets(self, kbid: str, request: CatalogFacetsRequest) -> dict[str, int]: ...
