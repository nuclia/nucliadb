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

import asyncio
import uuid

import pytest
from nucliadb_protos.resources_pb2 import Basic, ExtractedTextWrapper
from nucliadb_protos.utils_pb2 import ExtractedText
from redis import asyncio as aioredis

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.utils import set_basic
from nucliadb.search.search import paragraphs
from nucliadb.search.settings import settings


@pytest.fixture()
async def paragraph_cache(redis):
    settings.search_cache_redis_host = redis[0]
    settings.search_cache_redis_port = redis[1]
    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    cache = paragraphs.ParagraphsCache()
    await cache.initialize()
    yield cache
    await cache.finalize()
    settings.search_cache_redis_host = None
    settings.search_cache_redis_port = None
    await driver.close(close_connection_pool=True)


async def wait_for_queue(paragraph_cache: paragraphs.ParagraphsCache):
    while paragraph_cache.queue.qsize() > 0:
        await asyncio.sleep(0.01)


async def test_get_paragraph_cache_metrics(paragraph_cache: paragraphs.ParagraphsCache):
    try:
        paragraphs.CACHE_OPS.counter.clear()
        paragraphs.CACHE_HIT_DISTRIBUTION.histo.clear()
    except AttributeError:  # pragma: no cover
        # when no metrics are registered, this will fail
        pass

    assert (
        await paragraph_cache.get(
            kbid="1", rid="1", field="1", field_id="field_id", start=0, end=0, split="1"
        )
        is None
    )
    # RIGHT NOW IT SHOULD ALWAYS BE NONE! No Cache implemented yet!
    assert (
        await paragraph_cache.get(
            kbid="1", rid="1", field="1", field_id="field_id", start=0, end=0, split="1"
        )
        is None
    )

    await wait_for_queue(paragraph_cache)

    assert [
        samp
        for samp in paragraphs.CACHE_OPS.counter.collect()[0].samples  # type: ignore
        if samp.labels["type"] == "miss"
    ][0].value == 1.0

    assert [
        samp
        for samp in paragraphs.CACHE_OPS.counter.collect()[0].samples  # type: ignore
        if samp.labels["type"] == "hit"
    ][0].value == 1.0


async def test_get_paragraph_text(
    gcs_storage, cache, txn, fake_node, processor, knowledgebox_ingest
):
    kbid = knowledgebox_ingest
    uid = uuid.uuid4().hex
    basic = Basic(slug="slug", uuid=uid)
    await set_basic(txn, kbid, uid, basic)
    kb = KnowledgeBox(txn, gcs_storage, kbid)
    orm_resource = await kb.get(uid)
    field_obj = await orm_resource.get_field("field", 4, load=False)
    await field_obj.set_extracted_text(
        ExtractedTextWrapper(body=ExtractedText(text="Hello World!"))
    )

    # make sure to reset the cache
    field_obj.extracted_text = None

    text1 = await paragraphs.get_paragraph_text(
        kbid=kbid,
        rid=uid,
        field="/t/field",
        start=0,
        end=5,
        orm_resource=orm_resource,
    )
    assert text1 == "Hello"
