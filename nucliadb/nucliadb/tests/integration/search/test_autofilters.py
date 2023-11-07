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
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb_utils.utilities import Utility, set_utility


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_autofilters_are_returned(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)

    predict_mock.detect_entities = AsyncMock(
        return_value=[
            RelationNode(
                value="Becquer", ntype=RelationNode.NodeType.ENTITY, subtype="poet"
            ),
            RelationNode(
                value="Newton", ntype=RelationNode.NodeType.ENTITY, subtype="scientist"
            ),
        ]
    )
    predict_mock.convert_sentence_to_vector = AsyncMock(return_value=[0.1, 0.2, 0.3])

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search",
        params={
            "query": "What relates Newton and Becquer?",
        },
    )
    assert resp.status_code == 200
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
    assert "/entities/poet/Becquer" in autofilters
