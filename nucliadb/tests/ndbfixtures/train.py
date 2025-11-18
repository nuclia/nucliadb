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
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from time import time
from typing import TYPE_CHECKING, AsyncIterator
from unittest.mock import patch

import aiohttp
import pytest
from grpc import aio
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.resources import KB_RESOURCE_SLUG_BASE
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.nidx import NidxUtility
from nucliadb.ingest.orm.processor import Processor
from nucliadb.standalone.settings import Settings
from nucliadb.train.settings import settings as train_settings
from nucliadb.train.utils import (
    start_shard_manager,
    start_train_grpc,
    stop_shard_manager,
    stop_train_grpc,
)
from nucliadb_protos.knowledgebox_pb2 import Label, LabelSet
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldEntity,
    FieldID,
    FieldType,
    Paragraph,
    Position,
    Sentence,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.settings import (
    running_settings,
)
from nucliadb_utils.tests import free_port

if TYPE_CHECKING:
    from nucliadb_protos.train_pb2_grpc import TrainAsyncStub as TrainStub
else:
    from nucliadb_protos.train_pb2_grpc import TrainStub


@dataclass
class TrainGrpcServer:
    port: int


# Main fixtures


@pytest.fixture(scope="function")
async def component_nucliadb_train_grpc(train_grpc_server: TrainGrpcServer) -> AsyncIterator[TrainStub]:
    channel = aio.insecure_channel(f"localhost:{train_grpc_server.port}")
    stub = TrainStub(channel)
    yield stub
    await channel.close(grace=None)


@pytest.fixture(scope="function")
async def standalone_nucliadb_train_grpc(nucliadb: Settings) -> AsyncIterator[TrainStub]:
    channel = aio.insecure_channel(f"localhost:{nucliadb.train_grpc_port}")
    stub = TrainStub(channel)
    yield stub
    await channel.close(grace=None)


@pytest.fixture(scope="function")
async def standalone_nucliadb_train(standalone_nucliadb: Settings):
    async with aiohttp.ClientSession(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://localhost:{standalone_nucliadb.http_port}",
    ) as client:
        yield client


# Utils


@pytest.fixture(scope="function")
async def train_grpc_server(
    storage_settings,
    dummy_nidx_utility: NidxUtility,
    maindb_driver: Driver,
) -> AsyncIterator[TrainGrpcServer]:
    with (
        patch.object(running_settings, "debug", False),
        patch.object(train_settings, "grpc_port", free_port()) as grpc_port,
    ):
        await start_shard_manager()
        await start_train_grpc("testing_train")

        yield TrainGrpcServer(
            port=grpc_port,
        )

        await stop_train_grpc()
        await stop_shard_manager()


# Resources


@pytest.fixture(scope="function")
async def knowledgebox_with_labels(nucliadb_writer: AsyncClient, knowledgebox: str):
    resp = await nucliadb_writer.post(
        f"kb/{knowledgebox}/labelset/labelset_paragraphs",
        json={
            "kind": ["PARAGRAPHS"],
            "labels": [{"title": "label_machine"}, {"title": "label_user"}],
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"kb/{knowledgebox}/labelset/labelset_resources",
        json={
            "kind": ["RESOURCES"],
            "labels": [{"title": "label_machine"}, {"title": "label_user"}],
        },
    )
    assert resp.status_code == 200

    yield knowledgebox


def broker_simple_resource(knowledgebox: str, number: int) -> BrokerMessage:
    rid = str(uuid.uuid4())
    message1: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=str(number),
        type=BrokerMessage.AUTOCOMMIT,
    )

    message1.basic.slug = str(number)
    message1.basic.icon = "text/plain"
    message1.basic.title = f"MY TITLE {number}"
    message1.basic.summary = "Summary of document"
    message1.basic.thumbnail = "doc"
    message1.basic.metadata.useful = True
    message1.basic.metadata.language = "es"
    message1.basic.created.FromDatetime(datetime.now(timezone.utc))
    message1.basic.modified.FromDatetime(datetime.now(timezone.utc))
    message1.texts[
        "field1"
    ].body = "My lovely field with some information from Barcelona. This will be the good field. \n\n And then we will go Manresa."  # noqa
    message1.source = BrokerMessage.MessageSource.WRITER
    return message1


def broker_processed_resource(knowledgebox, number, rid) -> BrokerMessage:
    message2: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=str(number),
        type=BrokerMessage.AUTOCOMMIT,
    )
    message2.basic.metadata.useful = True
    message2.basic.metadata.language = "es"
    message2.source = BrokerMessage.MessageSource.PROCESSOR

    field1_if = FieldID()
    field1_if.field = "field1"
    field1_if.field_type = FieldType.TEXT

    title_if = FieldID()
    title_if.field = "title"
    title_if.field_type = FieldType.GENERIC

    etw = ExtractedTextWrapper()
    etw.field.CopyFrom(field1_if)
    etw.body.text = "My lovely field with some information from Barcelona. This will be the good field. \n\n And then we will go Manresa. I miss Manresa!"  # noqa
    message2.extracted_text.append(etw)

    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(field1_if)
    p1 = Paragraph()
    p1.start = 0
    p1.end = 82
    s1 = Sentence()
    s1.start = 0
    s1.end = 52
    p1.sentences.append(s1)
    s1 = Sentence()
    s1.start = 53
    s1.end = 82
    p1.sentences.append(s1)

    p2 = Paragraph()
    p2.start = 84
    p2.end = 130

    s1 = Sentence()
    s1.start = 84
    s1.end = 130
    p2.sentences.append(s1)

    fcmw.metadata.metadata.paragraphs.append(p1)
    fcmw.metadata.metadata.paragraphs.append(p2)

    # Data Augmentation + Processor entities
    fcmw.metadata.metadata.entities["my-task-id"].entities.extend(
        [
            FieldEntity(text="Barcelona", label="CITY", positions=[Position(start=43, end=52)]),
            FieldEntity(text="Manresa", label="CITY"),
        ]
    )
    # Legacy processor entities
    # TODO: Remove once processor doesn't use this anymore and remove the positions and ner fields from the message
    # Add a ner with positions
    fcmw.metadata.metadata.ner.update(
        {
            "Barcelona": "CITY",
            "Manresa": "CITY",
        }
    )
    fcmw.metadata.metadata.positions["CITY/Barcelona"].entity = "Barcelona"
    fcmw.metadata.metadata.positions["CITY/Barcelona"].position.append(Position(start=43, end=52))

    message2.field_metadata.append(fcmw)

    etw = ExtractedTextWrapper()
    etw.field.CopyFrom(title_if)
    etw.body.text = f"MY TITLE {number}"
    message2.extracted_text.append(etw)

    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(title_if)
    p1 = Paragraph()
    p1.start = 0
    p1.end = len(etw.body.text)
    s1 = Sentence()
    s1.start = 0
    s1.end = len(etw.body.text)
    p1.sentences.append(s1)
    fcmw.metadata.metadata.paragraphs.append(p1)
    message2.field_metadata.append(fcmw)
    message2.basic.metadata.language = "es"

    return message2


# This fixtures should be deleted once grpc train interface is removed


@pytest.fixture(scope="function")
async def test_pagination_resources(processor: Processor, knowledgebox: str):
    """
    Create a set of resources with only basic information to test pagination
    """
    amount = 10

    # Create resources
    for i in range(1, amount + 1):
        message = broker_simple_resource(knowledgebox, i)
        await processor.process(message=message, seqid=-1, transaction_check=False)

        message = broker_processed_resource(knowledgebox, i, message.uuid)
        await processor.process(message=message, seqid=-1, transaction_check=False)
        # Give processed data some time to reach the node

    driver = get_driver()

    t0 = time()

    while time() - t0 < 30:  # wait max 30 seconds for it
        async with driver.ro_transaction() as txn:
            count = 0
            async for key in txn.keys(match=KB_RESOURCE_SLUG_BASE.format(kbid=knowledgebox)):
                count += 1

        if count == amount:
            break
        print(f"got {count}, retrying")
        await asyncio.sleep(2)

    # Add ontology
    labelset = LabelSet()
    labelset.title = "ls1"
    label = Label()
    label_title = "label1"
    label.title = label_title
    labelset.labels.append(label)
    await datamanagers.atomic.labelset.set(kbid=knowledgebox, labelset_id="ls1", labelset=labelset)

    yield knowledgebox
