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

import json

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage, OpStatusWriter

from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc
from nucliadb_utils.cache import KB_COUNTER_CACHE
from nucliadb_utils.utilities import get_cache


@pytest.mark.asyncio
async def test_process_message_clears_counters_cache(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    # Create a new KB
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
    pb.config.title = "My Title"
    result: knowledgebox_pb2.NewKnowledgeBoxResponse = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK
    kbid = result.uuid

    # Set some values in the KB counters cache
    cache = await get_cache()
    assert cache is not None
    kb_counters_key = KB_COUNTER_CACHE.format(kbid=kbid)
    await cache.set(
        kb_counters_key,
        json.dumps(
            {
                "resources": 100,
                "paragraphs": 100,
                "fields": 100,
                "sentences": 100,
                "shards": [],
            }
        ),
    )
    assert await cache.get(kb_counters_key)

    # Create a BM to process
    bm = BrokerMessage()
    bm.uuid = "test1"
    bm.slug = bm.basic.slug = "slugtest"
    bm.kbid = kbid
    bm.texts["text1"].body = "My text1"

    # Check that the cache is currently empty
    resp = await stub.ProcessMessage([bm])  # type: ignore
    assert resp.status == OpStatusWriter.Status.OK

    # Check that KB counters cache is empty
    assert await cache.get(kb_counters_key) is None
