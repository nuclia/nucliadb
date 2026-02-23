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
from unittest.mock import AsyncMock, patch

from nucliadb.search.search.query_parser.fetcher import Fetcher


async def test_matryoshka_dimension_cache() -> None:
    with (
        patch("nucliadb.search.search.query_parser.fetcher.get_matryoshka_dimension") as mock,
    ):
        fetcher = new_fetcher()
        # multiple calls with the same fetcher are cached
        await fetcher.get_matryoshka_dimension()
        await fetcher.get_matryoshka_dimension()
        assert mock.call_count == 1

        # another fetcher instance reuses the cached matryoshka dimension
        fetcher = new_fetcher()
        await fetcher.get_matryoshka_dimension()
        assert mock.call_count == 1


async def test_validate_vectorset_cache() -> None:
    with (
        patch("nucliadb.search.search.query_parser.fetcher.datamanagers.with_ro_transaction") as mock,
        patch("nucliadb.search.search.query_parser.fetcher.datamanagers.vectorsets", new=AsyncMock()),
    ):
        fetcher = new_fetcher()
        # vectorset validation is cached per fetcher
        await fetcher.validate_vectorset("kbid", "vectorset")
        await fetcher.validate_vectorset("kbid", "vectorset")
        assert mock.call_count == 1

        fetcher = new_fetcher()
        # but not across requests (to avoid need for invalidation)
        await fetcher.validate_vectorset("kbid", "vectorset")
        assert mock.call_count == 2


def new_fetcher() -> Fetcher:
    vectorset = "my-vectorset"
    fetcher = Fetcher(
        "kbid",
        query="query",
        user_vector=None,
        vectorset=vectorset,
        rephrase=False,
        rephrase_prompt=None,
        generative_model=None,
        query_image=None,
    )
    fetcher.get_vectorset = AsyncMock(return_value=vectorset)  # type: ignore
    return fetcher
