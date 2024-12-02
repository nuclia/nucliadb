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

from pydantic import (
    BaseModel,
    Field,
)

from nucliadb_models import search as search_models

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


class MultiMatchBoosterReranker(Reranker): ...


class PredictReranker(Reranker):
    window: int = Field(le=200)


# retrieval operation


@dataclass
class UnitRetrieval:
    top_k: int
    rank_fusion: RankFusion
    reranker: Reranker
