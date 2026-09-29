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
import random
import re
from collections.abc import AsyncIterable

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.nidx import NidxBindingUtility, NidxServiceUtility, get_nidx
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.nidx import SEARCHER_REFRESH_INTERVAL_SECONDS
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import mark_dirty, wait_for_sync


# Used by: nucliadb writer tests
@pytest.fixture(scope="function")
async def resource(nucliadb_writer: AsyncClient, knowledgebox: str):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/resources",
        json={
            "slug": "resource1",
            "title": "Resource 1",
        },
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    return uuid


@pytest.fixture(scope="function")
async def simple_resources(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> AsyncIterable[tuple[str, list[str]]]:
    kbid = knowledgebox
    rids = [
        await create_simple_resource(kbid, f"my simple {i}", nucliadb_writer, nucliadb_ingest_grpc)
        for i in range(10)
    ]

    nidx = get_nidx()
    if isinstance(nidx, NidxServiceUtility):
        # wait_for_sync is not useful, we wait for the searcher to reload the indexed data
        await asyncio.sleep(SEARCHER_REFRESH_INTERVAL_SECONDS)
    elif isinstance(nidx, NidxBindingUtility):
        await mark_dirty()
        await wait_for_sync()
    else:
        # dummy/mocked nidx
        pass

    yield kbid, rids


async def create_simple_resource(
    kbid: str,
    name: str,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
) -> str:
    slug = f"simple-resource::{slugify(name)}"
    field_id = "simple-text"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "slug": slug,
            "title": name,
            "origin": {
                "metadata": {
                    "name": name,
                }
            },
            "texts": {
                field_id: {
                    "body": f"A simple resource text: {name}",
                    "format": "PLAIN",
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    vectorsets = {}
    async with datamanagers.with_ro_transaction() as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vectorsets[vectorset_id] = vs
    # use a controlled random seed for vector generation
    random.seed(23)

    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        slug=slug,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    bmb.with_title(name)
    bmb.with_summary(f"A summary for {name}")

    text_builder = bmb.field_builder(field_id, FieldType.TEXT)
    text_builder.add_paragraph(
        f"A simple resource processed text: {name}",
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )

    bm = bmb.build()
    bm.origin.metadata["name"] = name

    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid


def slugify(s: str) -> str:
    # replace non-word chars (not numbers, letters or underscore) for spaces
    s = re.sub(r"\W", " ", s, flags=re.ASCII)
    # replace multiple spaces for a single one
    s = re.sub(" +", " ", s)
    # convert spaces to dashes
    s = re.sub(" ", "-", s)
    return s
