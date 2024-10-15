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
from httpx import AsyncClient

from nucliadb_models.search import Reranker


@pytest.mark.parametrize(
    "reranker", [Reranker.MULTI_MATCH_BOOSTER, Reranker.PREDICT_RERANKER, Reranker.NOOP]
)
async def test_reranker(
    nucliadb_reader: AsyncClient,
    philosophy_books_kb: str,
    reranker: str,
):
    kbid = philosophy_books_kb

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "Which is our future?",
            "reranker": reranker,
        },
    )
    assert resp.status_code == 200
