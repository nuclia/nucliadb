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
import time
from unittest.mock import AsyncMock, Mock

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.integration.search.test_search import (
    broker_resource_with_classifications,
)
from nucliadb.tests.utils import inject_message
from nucliadb_utils.tests.asyncbenchmark import AsyncBenchmarkFixture
from nucliadb_utils.utilities import Utility, set_utility


@pytest.mark.benchmark(
    group="search",
    min_time=0.1,
    max_time=0.5,
    min_rounds=5,
    timer=time.time,
    disable_gc=True,
    warmup=False,
)
@pytest.mark.asyncio
async def test_search_returns_labels(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
    asyncbenchmark: AsyncBenchmarkFixture,
):
    bm = broker_resource_with_classifications(knowledgebox)
    await inject_message(nucliadb_grpc, bm)

    resp = await asyncbenchmark(
        nucliadb_reader.get,
        f"/kb/{knowledgebox}/search?query=Some&show=extracted&extracted=metadata",
    )
    assert resp.status_code == 200


@pytest.mark.benchmark(
    group="search",
    min_time=0.1,
    max_time=0.5,
    min_rounds=5,
    timer=time.time,
    disable_gc=True,
    warmup=False,
)
@pytest.mark.asyncio
async def test_search_relations(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
    knowledge_graph,
    asyncbenchmark: AsyncBenchmarkFixture,
):
    relation_nodes, relation_edges = knowledge_graph

    predict_mock = Mock()
    set_utility(Utility.PREDICT, predict_mock)

    predict_mock.detect_entities = AsyncMock(
        return_value=[relation_nodes["Becquer"], relation_nodes["Newton"]]
    )

    resp = await asyncbenchmark(
        nucliadb_reader.get,
        f"/kb/{knowledgebox}/search",
        params={
            "features": "relations",
            "query": "What relates Newton and Becquer?",
        },
    )
    assert resp.status_code == 200
