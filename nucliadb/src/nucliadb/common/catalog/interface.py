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
from typing import Literal, Optional, Union

from nidx_protos.noderesources_pb2 import Resource as IndexMessage
from pydantic import BaseModel, Field

from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.orm.resource import Resource
from nucliadb_models import search as search_models
from nucliadb_models.search import CatalogFacetsRequest, Resources


class CatalogResourceData(BaseModel):
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
        field: Union[Literal["created_at"], Literal["modified_at"]]
        since: Optional[datetime.datetime]
        until: Optional[datetime.datetime]

    bool_and: Optional[list["CatalogExpression"]] = None
    bool_or: Optional[list["CatalogExpression"]] = None
    bool_not: Optional["CatalogExpression"] = None
    date: Optional[Date] = None
    facet: Optional[str] = None
    resource_id: Optional[str] = None


class CatalogQuery(BaseModel):
    kbid: str
    query: Optional[search_models.CatalogQuery]
    filters: Optional[CatalogExpression]
    sort: search_models.SortOptions
    faceted: list[str]
    page_size: int
    page_number: int


class Catalog(abc.ABC, metaclass=abc.ABCMeta):
    def get_resource_data(self, resource: Resource, index_message: IndexMessage) -> CatalogResourceData:
        if resource.basic is None:
            raise ValueError("Cannot index into the catalog a resource without basic metadata ")

        created_at = resource.basic.created.ToDatetime()
        modified_at = resource.basic.modified.ToDatetime()
        if modified_at < created_at:
            modified_at = created_at

        # Do not index canceled labels
        cancelled_labels = {
            f"/l/{clf.labelset}/{clf.label}"
            for clf in resource.basic.usermetadata.classifications
            if clf.cancelled_by_user
        }

        # Labels from the resource and classification labels from each field
        labels = [label for label in index_message.labels]
        for classification in resource.basic.computedmetadata.field_classifications:
            for clf in classification.classifications:
                label = f"/l/{clf.labelset}/{clf.label}"
                if label not in cancelled_labels:
                    labels.append(label)

        return CatalogResourceData(
            title=resource.basic.title,
            created_at=created_at,
            modified_at=modified_at,
            labels=labels,
            slug=resource.basic.slug,
        )

    @staticmethod
    def extract_facets(labels: list[str]) -> set[str]:
        facets = set()
        for label in labels:
            parts = label.split("/")
            facet = ""
            for part in parts[1:]:
                facet += f"/{part}"
                facets.add(facet)
        return facets

    async def update(self, txn: Transaction, kbid: str, resource: Resource, index_message: IndexMessage):
        await self._update(txn, kbid, resource.uuid, self.get_resource_data(resource, index_message))

    @abc.abstractmethod
    async def _update(self, txn: Transaction, kbid: str, rid: str, data: CatalogResourceData): ...

    @abc.abstractmethod
    async def delete(self, txn: Transaction, kbid: str, rid: str): ...

    @abc.abstractmethod
    async def search(self, query: CatalogQuery) -> Resources: ...

    @abc.abstractmethod
    async def facets(self, kbid: str, request: CatalogFacetsRequest) -> dict[str, int]: ...
