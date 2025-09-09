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
import base64
import hashlib

import pytest
from httpx import AsyncClient
from nidx_protos import nodewriter_pb2

from nucliadb.common import datamanagers
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.nidx import get_nidx
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import dirty_index, inject_message


@pytest.mark.deploy_modes("standalone")
async def test_reindex(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    await _test_reindex(nucliadb_reader, nucliadb_writer, nucliadb_ingest_grpc, standalone_knowledgebox)


@pytest.mark.deploy_modes("standalone")
async def test_reindex_kb_with_vectorsets(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox_with_vectorsets: str,
):
    await _test_reindex(
        nucliadb_reader, nucliadb_writer, nucliadb_ingest_grpc, knowledgebox_with_vectorsets
    )


async def _test_reindex(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    kbid,
):
    rid = await create_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    # Doing a search should return results
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search?query=text")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["sentences"]["results"]) > 0
    assert len(content["paragraphs"]["results"]) > 0

    # Clean the indexes without touching maindb
    shard_manager = KBShardManager()
    shards = await shard_manager.get_shards_by_kbid(kbid)
    await shard_manager.delete_resource(shards[0], rid, 1, "", kbid)

    nidx = get_nidx()
    if nidx:
        for shard in await shard_manager.get_shards_by_kbid(kbid):
            msg = nodewriter_pb2.IndexMessage(
                shard=shard.nidx_shard_id, typemessage=nodewriter_pb2.DELETION, resource=rid
            )
            await nidx.index(msg)
        await dirty_index.mark_dirty()

    await asyncio.sleep(0.5)

    # Doing a search should not return any result now
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search?query=My+own")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["sentences"]["results"]) == 0
    assert len(content["paragraphs"]["results"]) == 0

    # Then do a reindex of the resource with its vectors
    resp = await nucliadb_writer.post(f"/kb/{kbid}/resource/{rid}/reindex?reindex_vectors=true")
    assert resp.status_code == 200

    await asyncio.sleep(0.5)

    # Doing a search should return semantic results
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search?query=My+own")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["sentences"]["results"]) > 0
    assert len(content["paragraphs"]["results"]) > 0


async def create_resource(kbid: str, nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub):
    # create resource
    file_content = b"This is a file"
    field_id = "myfile"
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": "my-resource",
            "title": "My resource",
            "files": {
                field_id: {
                    "language": "en",
                    "file": {
                        "filename": "testfile",
                        "content_type": "text/plain",
                        "payload": base64.b64encode(file_content).decode("utf-8"),
                        "md5": hashlib.md5(file_content).hexdigest(),
                    },
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # update it with extracted data
    bm = await broker_resource(kbid, rid)
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    await inject_message(nucliadb_ingest_grpc, bm)
    return bm.uuid


async def broker_resource(kbid: str, rid: str) -> BrokerMessage:
    from nucliadb.tests.vectors import V1, V2, V3
    from nucliadb_protos import resources_pb2 as rpb
    from tests.utils.broker_messages import BrokerMessageBuilder

    bmb = BrokerMessageBuilder(kbid=kbid, rid=rid)
    bmb.with_title("Title Resource")
    bmb.with_summary("Summary of document")

    file_field = bmb.field_builder("myfile", rpb.FieldType.FILE)
    file_field.with_extracted_text("My own text Ramon. This is great to be here. \n Where is my beer?")
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

    async with datamanagers.with_ro_transaction() as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            dimension = vs.vectorset_index_config.vector_dimension
            padding = dimension - len(V1)
            vectors = [
                rpb.Vector(
                    start=0,
                    end=19,
                    start_paragraph=0,
                    end_paragraph=45,
                    vector=V1 + [1.0] * padding,
                ),
                rpb.Vector(
                    start=20,
                    end=45,
                    start_paragraph=0,
                    end_paragraph=45,
                    vector=V2 + [1.0] * padding,
                ),
                rpb.Vector(
                    start=48,
                    end=65,
                    start_paragraph=47,
                    end_paragraph=64,
                    vector=V3 + [1.0] * padding,
                ),
            ]
            file_field.with_extracted_vectors(vectors, vectorset_id)

    bm = bmb.build()

    return bm
