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

import pytest
from aioresponses import aioresponses
from nucliadb_protos.train_pb2 import GetInfoRequest, TrainInfo
from nucliadb_protos.train_pb2_grpc import TrainStub


@pytest.mark.asyncio
async def test_get_info(
    train_client: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetInfoRequest()
    req.kb.uuid = knowledgebox

    with aioresponses() as m:
        m.get(
            f"http://search.nuclia.svc.cluster.local:8030/api/v1/kb/{knowledgebox}/counters",
            payload={"resources": 4, "paragraphs": 89, "fields": 4, "sentences": 90},
        )

        labels: TrainInfo = await train_client.GetInfo(req)  # type: ignore
    assert labels.fields == 4
    assert labels.resources == 4
    assert labels.paragraphs == 89
    assert labels.sentences == 90
