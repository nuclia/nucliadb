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
import asyncio
from functools import partial

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.tests.utils import inject_message
from nucliadb_protos import noderesources_pb2


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_reindex(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    rid = await create_resource(knowledgebox, nucliadb_grpc)

    # Doing a search should return results
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=text")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["sentences"]["results"]) > 0
    assert len(content["paragraphs"]["results"]) > 0

    async def clean_shard(
        resources: list[str], node: AbstractIndexNode, shard_replica_id: str
    ):
        nonlocal rid
        return await node.writer.RemoveResource(  # type: ignore
            noderesources_pb2.ResourceID(
                shard_id=shard_replica_id,
                uuid=rid,
            )
        )

    shard_manager = KBShardManager()
    results = await shard_manager.apply_for_all_shards(
        knowledgebox, partial(clean_shard, [rid]), timeout=5
    )
    for result in results:
        assert not isinstance(result, Exception)

    await asyncio.sleep(0.5)

    # Doing a search should not return any result now
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=My+own")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["sentences"]["results"]) == 0
    assert len(content["paragraphs"]["results"]) == 0

    # Then do a reindex of the resource with its vectors
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resource/{rid}/reindex?reindex_vectors=true"
    )
    assert resp.status_code == 200

    await asyncio.sleep(0.5)

    # Doing a search should return semantic results
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=My+own")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["sentences"]["results"]) > 0
    assert len(content["paragraphs"]["results"]) > 0


async def create_resource(knowledgebox, writer: WriterStub):
    bm = broker_resource(knowledgebox)
    await inject_message(writer, bm)
    return bm.uuid


def broker_resource(knowledgebox: str) -> BrokerMessage:
    from nucliadb.ingest.tests.vectors import V1, V2, V3
    from nucliadb.tests.utils.broker_messages import BrokerMessageBuilder, FieldBuilder
    from nucliadb_protos import resources_pb2 as rpb

    bmb = BrokerMessageBuilder(kbid=knowledgebox)
    bmb.with_title("Title Resource")
    bmb.with_summary("Summary of document")

    file_field = FieldBuilder("file", rpb.FieldType.FILE)
    file_field.with_extracted_text(
        "My own text Ramon. This is great to be here. \n Where is my beer?"
    )
    file_field.with_extracted_paragraph_metadata(
        rpb.Paragraph(
            start=0,
            end=45,
        )
    )
    file_field.with_extracted_paragraph_metadata(
        rpb.Paragraph(
            start=47,
            end=64,
        )
    )

    bmb.add_field_builder(file_field)
    bm = bmb.build()

    ev = rpb.ExtractedVectorsWrapper()
    ev.field.field = "file"
    ev.field.field_type = rpb.FieldType.FILE

    v1 = rpb.Vector()
    v1.start = 0
    v1.end = 19
    v1.start_paragraph = 0
    v1.end_paragraph = 45
    v1.vector.extend(V1)
    ev.vectors.vectors.vectors.append(v1)

    v2 = rpb.Vector()
    v2.start = 20
    v2.end = 45
    v2.start_paragraph = 0
    v2.end_paragraph = 45
    v2.vector.extend(V2)
    ev.vectors.vectors.vectors.append(v2)

    v3 = rpb.Vector()
    v3.start = 48
    v3.end = 65
    v3.start_paragraph = 47
    v3.end_paragraph = 64
    v3.vector.extend(V3)
    ev.vectors.vectors.vectors.append(v3)

    bm.field_vectors.append(ev)
    return bm
