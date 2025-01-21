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

from httpx import AsyncClient

from nucliadb_protos.resources_pb2 import ExtractedTextWrapper, ExtractedVectorsWrapper
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
)
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


async def test_da_cleanup(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox: str,
):
    # Create a resource
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "Walter White",
            "texts": {
                "text1": {
                    "body": "I am the one who knocks",
                }
            }
        }
    )
    assert resp.status_code == 201
    body = await resp.json()
    rid = body["uuid"]

    # Simulate processing broker message
    bm = BrokerMessage()
    bm.kbid = knowledgebox
    bm.uuid = rid
    etw = ExtractedTextWrapper()
    bm.extracted_text.append(etw)
    evw = ExtractedVectorsWrapper()
    bm.field_vectors.append(evw)
    await inject_message(nucliadb_grpc, bm)

    # Simulate all kinds of data augmentation on the kb

    # labeler
    bm = BrokerMessage(kbid=knowledgebox, uuid=rid)
    await inject_message(nucliadb_grpc, bm)

    # graph
    bm = BrokerMessage(kbid=knowledgebox, uuid=rid)
    await inject_message(nucliadb_grpc, bm)

    # generator
    bm = BrokerMessage(kbid=knowledgebox, uuid=rid)
    await inject_message(nucliadb_grpc, bm)

    # q&a
    bm = BrokerMessage(kbid=knowledgebox, uuid=rid)
    await inject_message(nucliadb_grpc, bm)

    # Make sure search works as expected on all cases

    # Simulate data augmentation cleanup

    # Make sure search works as expected after the cleanup
    pass
