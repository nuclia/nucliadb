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

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional, Union

from pydantic import (
    BaseModel,
    Field,
)

from nucliadb_models import search as search_models
from nucliadb_protos import nodereader_pb2

### Retrieval

# filters


class DateTimeFilter(BaseModel):
    after: Optional[datetime] = None  # aka, start
    before: Optional[datetime] = None  # aka, end


# rank fusion


class RankFusion(BaseModel):
    window: int = Field(le=500)


class ReciprocalRankFusion(RankFusion):
    k: float = Field(default=60.0)
    boosting: search_models.ReciprocalRankFusionWeights = Field(
        default_factory=search_models.ReciprocalRankFusionWeights
    )


# reranking


class Reranker(BaseModel): ...


class NoopReranker(Reranker): ...


class PredictReranker(Reranker):
    window: int = Field(le=200)


# retrieval operation


@dataclass
class UnitRetrieval:
    top_k: int
    rank_fusion: RankFusion
    reranker: Reranker


### Catalog
@dataclass
class CatalogExpression:
    @dataclass
    class Date:
        field: Union[Literal["created_at"], Literal["modified_at"]]
        since: Optional[datetime]
        until: Optional[datetime]

    bool_and: Optional[list["CatalogExpression"]] = None
    bool_or: Optional[list["CatalogExpression"]] = None
    bool_not: Optional["CatalogExpression"] = None
    date: Optional[Date] = None
    facet: Optional[str] = None
    resource_id: Optional[str] = None


class CatalogQuery(BaseModel):
    kbid: str
    query: str
    filters: Optional[CatalogExpression]
    sort: search_models.SortOptions
    faceted: list[str]
    page_size: int
    page_number: int


### Graph


# Right now, we don't need a more generic model for graph queries, we can
# directly use the protobuffer directly
GraphRetrieval = nodereader_pb2.GraphSearchRequest
