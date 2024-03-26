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
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import AsyncClient

from nucliadb_models.search import Ner, QueryInfo, SentenceSearch, TokenSearch
from nucliadb_utils.utilities import Utility, set_utility


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_autofilters_are_returned(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
    knowledge_graph,
):
    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)

    predict_mock.query = AsyncMock(
        return_value=QueryInfo(
            entities=TokenSearch(
                tokens=[
                    Ner(text="Newton", ner="scientist", start=0, end=1),
                    Ner(text="Becquer", ner="poet", start=0, end=1),
                ],
                time=0.1,
            ),
            sentence=SentenceSearch(data=[0.1, 0.2, 0.3], time=0.1),
            visual_llm=False,
            max_context=10000,
            query="What relates Newton and Becquer?",
        )
    )

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search",
        params={
            "query": "What relates Newton and Becquer?",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["autofilters"] == []

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/find",
        params={
            "autofilter": True,
            "query": "What relates Newton and Becquer?",
        },
    )
    assert resp.status_code == 200
    autofilters = resp.json()["autofilters"]
    assert "/entities/scientist/Newton" in autofilters
    assert "/entities/scientist/Isaac Newton" in autofilters
    assert "/entities/poet/Becquer" in autofilters
    assert "/entities/poet/Gustavo Adolfo Bécquer" in autofilters
    # should also not include deleted entities
    assert "/entities/scientist/Isaac Newsome" not in autofilters
