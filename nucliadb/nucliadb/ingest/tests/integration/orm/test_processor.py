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
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxConfig

from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.tests.fixtures import IngestFixture


@pytest.mark.asyncio
async def test_create_knowledgebox(grpc_servicer: IngestFixture):
    count = 0
    async for key in grpc_servicer.servicer.proc.list_kb(""):
        count += 1
    assert count == 0

    kbid = await grpc_servicer.servicer.proc.create_kb(
        "test", KnowledgeBoxConfig(title="My Title 1")
    )
    assert kbid

    with pytest.raises(KnowledgeBoxConflict):
        kbid = await grpc_servicer.servicer.proc.create_kb(
            "test", KnowledgeBoxConfig(title="My Title 2")
        )

    kbid2 = await grpc_servicer.servicer.proc.create_kb(
        "test2", KnowledgeBoxConfig(title="My Title 3")
    )
    assert kbid2

    count = 0
    async for key in grpc_servicer.servicer.proc.list_kb(""):
        count += 1
    assert count == 2

    kbid = await grpc_servicer.servicer.proc.delete_kb(kbid=kbid2)
    assert kbid

    count = 0
    async for key in grpc_servicer.servicer.proc.list_kb(""):
        count += 1
    assert count == 1
