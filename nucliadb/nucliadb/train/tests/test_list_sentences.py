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
from nucliadb_protos.train_pb2 import GetSentencesRequest
from nucliadb_protos.train_pb2_grpc import TrainStub


@pytest.mark.asyncio
async def test_list_sentences(
    train_client: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetSentencesRequest()
    req.kb.uuid = knowledgebox
    req.metadata.entities = True
    req.metadata.labels = True
    req.metadata.text = True
    req.metadata.vector = True
    count = 0

    async for _ in train_client.GetSentences(req):  # type: ignore
        count += 1

    assert count == 40


@pytest.mark.asyncio
async def test_list_sentences_shows_ners_with_positions(
    train_client: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetSentencesRequest()
    req.kb.uuid = knowledgebox
    req.metadata.entities = True
    async for sentence in train_client.GetSentences(req):  # type: ignore
        if "Barcelona" in sentence.metadata.text:
            # Entity in text with positions
            assert sentence.metadata.entities == {"Barcelona": "CITY"}
            assert (
                sentence.metadata.entity_positions["CITY/Barcelona"].entity
                == "Barcelona"
            )
            assert (
                sentence.metadata.entity_positions["CITY/Barcelona"].positions[0].start
                == 43
            )
            assert (
                sentence.metadata.entity_positions["CITY/Barcelona"].positions[0].end
                == 52
            )
        elif "Manresa" in sentence.metadata.text:
            # Entity found in text but without positions
            assert sentence.metadata.entities == {"Manresa": "CITY"}
            assert "CITY/Manresa" not in sentence.metadata.entity_positions
        else:
            # Other sentences should not have entities nor positions
            assert sentence.metadata.entities == {}
            assert sentence.metadata.entity_positions == {}
