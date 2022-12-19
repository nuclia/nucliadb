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
from nucliadb_protos.train_pb2 import GetParagraphsRequest
from nucliadb_protos.train_pb2_grpc import TrainStub


@pytest.mark.asyncio
async def test_list_paragraphs(
    train_client: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetParagraphsRequest()
    req.kb.uuid = knowledgebox
    req.metadata.entities = True
    req.metadata.labels = True
    req.metadata.text = True
    req.metadata.vector = True
    count = 0
    async for _ in train_client.GetParagraphs(req):  # type: ignore
        count += 1

    assert count == 30


@pytest.mark.asyncio
async def test_list_paragraphs_shows_ners_with_positions(
    train_client: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetParagraphsRequest()
    req.kb.uuid = knowledgebox
    req.metadata.entities = True
    req.metadata.labels = True
    req.metadata.text = True
    req.metadata.vector = True

    found_barcelona = found_manresa = False
    async for paragraph in train_client.GetParagraphs(req):  # type: ignore
        if "Barcelona" in paragraph.metadata.text:
            found_barcelona = True
            assert paragraph.metadata.entities == {"Barcelona": "CITY"}
            assert (
                paragraph.metadata.entity_positions["CITY/Barcelona"].entity
                == "Barcelona"
            )
            assert (
                paragraph.metadata.entity_positions["CITY/Barcelona"].positions[0].start
                == 43
            )
            assert (
                paragraph.metadata.entity_positions["CITY/Barcelona"].positions[0].end
                == 52
            )
        elif "Manresa" in paragraph.metadata.text:
            found_manresa = True
            assert paragraph.metadata.entities == {"Manresa": "CITY"}
            assert (
                paragraph.metadata.entity_positions["CITY/Manresa"].positions[0].start
                == 22
            )
            assert (
                paragraph.metadata.entity_positions["CITY/Manresa"].positions[0].end
                == 29
            )
    assert found_manresa and found_barcelona
