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

from nucliadb_protos.train_pb2 import GetLabelsRequest
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2 import GetLabelsResponse


@pytest.mark.deploy_modes("component")
async def test_get_ontology(
    nucliadb_train_grpc: TrainStub, knowledgebox_ingest: str, test_pagination_resources
) -> None:
    req = GetLabelsRequest()
    req.kb.uuid = knowledgebox_ingest

    labels: GetLabelsResponse = await nucliadb_train_grpc.GetOntology(req)  # type: ignore
    assert labels.labels.labelset["ls1"].labels[0].title == "label1"
