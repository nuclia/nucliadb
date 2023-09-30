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
from nucliadb_protos.writer_pb2 import ListMembersRequest, Member

from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import writer_pb2_grpc


@pytest.mark.asyncio
async def test_list_members(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore

    response = await stub.ListMembers(ListMembersRequest())  # type: ignore

    for member in response.members:
        assert member.type == Member.Type.IO
        assert isinstance(member.shard_count, int)
