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
import base64
import json
import os
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import pytest
from httpx import AsyncClient

from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb_protos.dataset_pb2 import ImageClassificationBatch, TaskType, TrainSet
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    FileExtractedData,
    FilePages,
    PageStructure,
    PageStructurePage,
    PageStructureToken,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.train.utils import get_batches_from_train_response_stream
from tests.utils import inject_message
from tests.utils.dirty_index import wait_for_sync

_dir = os.path.dirname(__file__)
_testdata_dir = os.path.join(_dir, "..", "..", "tests", "testdata")

INVOICE_FILENAME = os.path.join(_testdata_dir, "invoice.pdf")
INVOICE_SELECTIONS_FILENAME = os.path.join(_testdata_dir, "invoice_selections.json")


@pytest.mark.deploy_modes("standalone")
async def test_generation_image_classification(
    nucliadb_train: aiohttp.ClientSession,
    knowledgebox: str,
    image_classification_resource,
):
    async with nucliadb_train.get(f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset") as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.IMAGE_CLASSIFICATION
    trainset.batch_size = 10

    await asyncio.sleep(0.1)
    async with nucliadb_train.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(response, ImageClassificationBatch):
            batches.append(batch)
            assert len(batch.data) == 1
            selections = json.loads(batch.data[0].selections)
            assert selections["width"] == 10
            assert selections["height"] == 10
            assert len(selections["tokens"]) == 87
            assert len(selections["annotations"]) == 18
            assert batch.data[0].page_uri == "DUMMY-URI"
        assert len(batches) == 1


@pytest.fixture
async def image_classification_resource(
    nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub, knowledgebox: str
):
    kbid = knowledgebox
    field_id = "invoice"

    with open(INVOICE_SELECTIONS_FILENAME) as f:
        selections = json.load(f)
        assert len(selections["tokens"]) == 87
        assert len(selections["annotations"]) == 18

    fieldmetadata = generate_image_classification_fieldmetadata(selections, field_id)

    with open(INVOICE_FILENAME, "rb") as f:
        invoice_content = f.read()

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/resources",
        json={
            "title": "My invoice",
            "files": {
                field_id: {
                    "file": {
                        "filename": "invoice.pdf",
                        "content_type": "application/pdf",
                        "payload": base64.b64encode(invoice_content).decode(),
                    }
                }
            },
            "fieldmetadata": fieldmetadata,
        },
    )
    assert resp.status_code == 201
    body = resp.json()
    rid = body["uuid"]

    broker_message = generate_image_classification_broker_message(selections, kbid, rid, field_id)

    mock_set = AsyncMock(return_value=None)
    mock_get = AsyncMock(return_value=broker_message.file_extracted_data[0])
    with (
        patch("nucliadb.ingest.fields.file.File.set_file_extracted_data", new=mock_set) as _,
        patch("nucliadb.ingest.fields.file.File.get_file_extracted_data", new=mock_get) as _,
    ):
        await inject_message(nucliadb_ingest_grpc, broker_message, timeout=10, wait_for_ready=True)
        await wait_for_sync()
        yield


def generate_image_classification_fieldmetadata(selections: dict, field_id: str) -> list[dict[str, Any]]:
    selections_by_page = {}  # type: ignore
    for annotation in selections["annotations"]:
        page_selections = selections_by_page.setdefault(annotation["page"], [])
        page_selections.append(
            {
                "label": annotation["label"]["text"],
                "top": annotation["bounds"]["top"],
                "left": annotation["bounds"]["left"],
                "right": annotation["bounds"]["right"],
                "bottom": annotation["bounds"]["bottom"],
                "token_ids": [token["tokenIndex"] for token in annotation["tokens"]],
            }
        )

    fieldmetadata = {
        "field": {"field": field_id, "field_type": "file"},
        "selections": [
            {
                "page": page,
                "visual": selections,
            }
            for page, selections in selections_by_page.items()
        ],
    }
    return [fieldmetadata]


def generate_image_classification_broker_message(
    selections: dict, kbid: str, rid: str, field_id: str
) -> BrokerMessage:
    bm = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        source=BrokerMessage.MessageSource.PROCESSOR,
        file_extracted_data=[
            FileExtractedData(
                field=field_id,
                file_pages_previews=FilePages(
                    pages=[
                        CloudFile(uri="DUMMY-URI"),
                    ],
                    structures=[
                        PageStructure(
                            page=PageStructurePage(width=10, height=10),
                            tokens=[
                                PageStructureToken(
                                    x=token["x"],
                                    y=token["y"],
                                    width=token["width"],
                                    height=token["height"],
                                    text=token["text"],
                                    line=0,
                                )
                                for token in selections["tokens"]
                            ],
                        )
                    ],
                ),
            )
        ],
    )

    return bm
