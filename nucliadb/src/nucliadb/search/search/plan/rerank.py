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

from typing_extensions import Self

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.search.search.rerankers import (
    RerankableItem,
    Reranker,
    RerankingOptions,
)

from .models import ExecutionContext, PlanStep


class Rerank(PlanStep):
    def __init__(self, kbid: str, reranker: Reranker, reranking_options: RerankingOptions, top_k: int):
        self.kbid = kbid
        self.reranker = reranker
        self.reranking_options = reranking_options
        self.top_k = top_k

    def plan(self, uses: PlanStep[list[TextBlockMatch]]) -> Self:
        self.source = uses
        return self

    async def execute(self, context: ExecutionContext) -> list[TextBlockMatch]:
        # we assume text blocks have been hydrated. It'd be nice to enforce this
        # with types
        text_blocks = await self.source.execute(context)

        text_blocks_by_id: dict[
            str, TextBlockMatch
        ] = {}  # useful for faster access to text blocks later
        to_rerank = []
        for text_block in text_blocks:
            paragraph_id = text_block.paragraph_id.full()

            # If we find multiple results (from different indexes) with different
            # metadata, this statement will only get the metadata from the first on
            # the list. We assume metadata is the same on all indexes, otherwise
            # this would be a BUG
            text_blocks_by_id.setdefault(paragraph_id, text_block)

            to_rerank.append(
                RerankableItem(
                    id=paragraph_id,
                    score=text_block.score,
                    score_type=text_block.score_type,
                    content=text_block.text or "",  # TODO: add a warning, this shouldn't usually happen
                )
            )

        reranked = await self.reranker.rerank(to_rerank, self.reranking_options)

        # after reranking, we can cut to the number of results the user wants, so we
        # don't work with unnecessary stuff
        reranked = reranked[: self.top_k]

        # now get back the text block matches
        matches = []
        for item in reranked:
            paragraph_id = item.id
            score = item.score
            score_type = item.score_type

            text_block = text_blocks_by_id[paragraph_id]
            text_block.score = score
            text_block.score_type = score_type

            matches.append((paragraph_id, score))

        # TODO: we shouldn't need to sort here again
        matches.sort(key=lambda x: x[1], reverse=True)

        best_text_blocks = []
        for order, (paragraph_id, _) in enumerate(matches):
            text_block = text_blocks_by_id[paragraph_id]
            text_block.order = order
            best_text_blocks.append(text_block)

        return best_text_blocks

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "Rerank": self.source.explain(),
        }
