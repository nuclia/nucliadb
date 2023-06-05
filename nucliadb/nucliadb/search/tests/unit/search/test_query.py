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

from unittest.mock import AsyncMock, Mock

import pytest
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb.search.search.query import _parse_entities
from nucliadb_utils.utilities import Utility, set_utility


@pytest.fixture(scope="function")
def predict():
    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)
    predict_mock.detect_entities = AsyncMock(
        return_value=[
            RelationNode(
                value="John", ntype=RelationNode.NodeType.ENTITY, subtype="person"
            )
        ]
    )
    yield predict_mock


async def test_parse_entities_autofilter(predict):
    request = SearchRequest()
    await _parse_entities(request, "kbid", "John", autofilter=False)
    assert request.filter.tags == []

    await _parse_entities(request, "kbid", "John", autofilter=True)
    assert request.filter.tags == ["/e/person/John"]
