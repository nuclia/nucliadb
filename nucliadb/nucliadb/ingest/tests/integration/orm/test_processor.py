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
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxConfig, SemanticModelMetadata
from nucliadb_protos.utils_pb2 import VectorSimilarity

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.tests.fixtures import IngestFixture


@pytest.mark.asyncio
async def test_create_knowledgebox(
    grpc_servicer: IngestFixture,
    maindb_driver: Driver,
):
    count = await count_all_kbs(maindb_driver)
    assert count == 0

    model = SemanticModelMetadata(
        similarity_function=VectorSimilarity.COSINE,
        vector_dimension=384,
        default_min_score=1.2,
    )
    kbid = await grpc_servicer.servicer.proc.create_kb(
        "test", KnowledgeBoxConfig(title="My Title 1"), model
    )
    assert kbid

    with pytest.raises(KnowledgeBoxConflict):
        kbid = await grpc_servicer.servicer.proc.create_kb(
            "test", KnowledgeBoxConfig(title="My Title 2"), model
        )

    kbid2 = await grpc_servicer.servicer.proc.create_kb(
        "test2", KnowledgeBoxConfig(title="My Title 3"), model
    )
    assert kbid2

    count = await count_all_kbs(maindb_driver)
    assert count == 2

    kbid = await grpc_servicer.servicer.proc.delete_kb(kbid=kbid2)
    assert kbid

    count = await count_all_kbs(maindb_driver)
    assert count == 1


async def count_all_kbs(driver: Driver):
    count = 0
    async with driver.transaction(read_only=True) as txn:
        async for _ in datamanagers.kb.get_kbs(txn):
            count += 1
    return count
