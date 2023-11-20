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
import sys

import pytest
from aioresponses import aioresponses
from nucliadb_protos.train_pb2 import GetLabelsetsCountRequest, LabelsetsCount
from nucliadb_protos.train_pb2_grpc import TrainStub

VERSION = sys.version_info
PY_GEQ_3_11 = VERSION.major > 3 or VERSION.major == 3 and VERSION.minor >= 11


@pytest.mark.asyncio
@pytest.mark.skipif(
    PY_GEQ_3_11, reason="aioresponses not compatible with python 3.11 yet"
)
async def test_get_ontology_count(
    train_client: TrainStub, knowledgebox_ingest: str, test_pagination_resources
) -> None:
    req = GetLabelsetsCountRequest()
    req.kb.uuid = knowledgebox_ingest

    with aioresponses() as m:
        m.get(
            f"http://search.nuclia.svc.cluster.local:8030/api/v1/kb/{knowledgebox_ingest}/search?faceted=/l/my-labelset",  # noqa
            payload={
                "resources": {},
                "sentences": {"results": [], "facets": {}},
                "paragraphs": {
                    "results": [],
                    "facets": {
                        "/l/my-labelset": {
                            "facetresults": [
                                {"tag": "/l/my-labelset/Label 1", "total": 1}
                            ]
                        }
                    },
                },
                "fulltext": {"results": [], "facets": {}},
            },
        )

        req.resource_labelsets.append("my-labelset")
        labels: LabelsetsCount = await train_client.GetOntologyCount(req)  # type: ignore
    assert labels.labelsets["/l/my-labelset"].paragraphs["Label 1"] == 1
