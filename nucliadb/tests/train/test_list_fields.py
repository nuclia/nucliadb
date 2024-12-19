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

from nucliadb_protos.train_pb2 import GetFieldsRequest
from nucliadb_protos.train_pb2_grpc import TrainStub


@pytest.mark.deploy_modes("component")
async def test_list_fields(
    nucliadb_train_grpc: TrainStub, knowledgebox_ingest: str, test_pagination_resources
) -> None:
    req = GetFieldsRequest()
    req.kb.uuid = knowledgebox_ingest
    req.metadata.entities = True
    req.metadata.labels = True
    req.metadata.text = True
    req.metadata.vector = True
    count = 0
    async for _ in nucliadb_train_grpc.GetParagraphs(req):  # type: ignore
        count += 1

    assert count == 30
