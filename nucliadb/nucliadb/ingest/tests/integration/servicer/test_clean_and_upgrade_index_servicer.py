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

from uuid import uuid4

import pytest

from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_clean_and_upgrade_kb_index(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore

    kb_id = str(uuid4())
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test", forceuuid=kb_id)
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    req = knowledgebox_pb2.KnowledgeBoxID(uuid=kb_id)
    result = await stub.CleanAndUpgradeKnowledgeBoxIndex(req)  # type: ignore
