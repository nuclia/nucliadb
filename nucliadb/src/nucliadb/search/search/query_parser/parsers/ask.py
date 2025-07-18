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
from typing import Optional

from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.models import (
    Generation,
)
from nucliadb_models.search import AskRequest, MaxTokens


async def parse_ask(kbid: str, item: AskRequest, *, fetcher: Optional[Fetcher] = None) -> Generation:
    fetcher = fetcher or fetcher_for_ask(kbid, item)
    parser = _AskParser(kbid, item, fetcher)
    return await parser.parse()


def fetcher_for_ask(kbid: str, item: AskRequest) -> Fetcher:
    return Fetcher(
        kbid=kbid,
        query=item.query,
        user_vector=None,
        vectorset=item.vectorset,
        rephrase=item.rephrase,
        rephrase_prompt=None,
        generative_model=item.generative_model,
        query_image=item.query_image,
    )


class _AskParser:
    def __init__(self, kbid: str, item: AskRequest, fetcher: Fetcher):
        self.kbid = kbid
        self.item = item
        self.fetcher = fetcher

    async def parse(self) -> Generation:
        use_visual_llm = await self.fetcher.get_visual_llm_enabled()

        if self.item.max_tokens is None:
            max_tokens = None
        elif isinstance(self.item.max_tokens, int):
            max_tokens = MaxTokens(
                context=None,
                answer=self.item.max_tokens,
            )
        elif isinstance(self.item.max_tokens, MaxTokens):
            max_tokens = self.item.max_tokens
        else:  # pragma: nocover
            # This is a trick so mypy generates an error if this branch can be reached,
            # that is, if we are missing some ifs
            _a: int = "a"

        max_context_tokens = await self.fetcher.get_max_context_tokens(max_tokens)
        max_answer_tokens = self.fetcher.get_max_answer_tokens(max_tokens)

        return Generation(
            use_visual_llm=use_visual_llm,
            max_context_tokens=max_context_tokens,
            max_answer_tokens=max_answer_tokens,
        )
