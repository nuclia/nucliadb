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
from datetime import datetime

import pytest
from grpc import aio
from nucliadb_protos.knowledgebox_pb2 import EntitiesGroup, Label, LabelSet
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
    Sentence,
)
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG_BASE
from nucliadb_utils.utilities import (
    Utility,
    clear_global_cache,
    get_storage,
    set_utility,
)


def free_port() -> int:
    import socket

    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


@pytest.fixture(scope="function")
def test_settings_train(cache, gcs, fake_node, redis_driver):  # type: ignore
    from nucliadb.train.settings import settings
    from nucliadb_utils.settings import running_settings, storage_settings

    running_settings.debug = False
    print(f"Redis ready at {redis_driver.url}")

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = "gcs"
    storage_settings.gcs_bucket = "test_{kbid}"
    settings.grpc_port = free_port()

    set_utility(Utility.CACHE, cache)
    yield


@pytest.fixture(scope="function")
async def train_api(test_settings_train: None, local_files, event_loop):  # type: ignore
    from nucliadb.train.server import start_grpc

    finalizer = await start_grpc("testing_train")
    yield

    finalizer()


@pytest.fixture(scope="function")
async def train_client(train_api):  # type: ignore
    from nucliadb_protos.train_pb2_grpc import TrainStub

    from nucliadb.train.settings import settings

    channel = aio.insecure_channel(f"localhost:{settings.grpc_port}")
    yield TrainStub(channel)
    clear_global_cache()


def broker_simple_resource(knowledgebox, number):
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
    message1.basic.layout = "default"
    message1.basic.metadata.useful = True
    message1.basic.metadata.language = "es"
    message1.basic.created.FromDatetime(datetime.utcnow())
    message1.basic.modified.FromDatetime(datetime.utcnow())
    message1.texts[
        "field1"
    ].body = "My lovely field with some information from Barcelona. This will be the good field. \n\n And then we will go Manresa."  # noqa
    message1.source = BrokerMessage.MessageSource.WRITER
    return message1


def broker_processed_resource(knowledgebox, number, rid):
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
    etw.body.text = "My lovely field with some information from Barcelona. This will be the good field. \n\n And then we will go Manresa."  # noqa
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
    p2.end = 103
    s1 = Sentence()
    s1.start = 84
    s1.end = 103
    p2.sentences.append(s1)
    fcmw.metadata.metadata.paragraphs.append(p1)
    fcmw.metadata.metadata.paragraphs.append(p2)
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


@pytest.fixture(scope="function")
async def test_pagination_resources(processor, knowledgebox, test_settings_train):
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

    from time import time

    from nucliadb.ingest.utils import get_driver

    driver = await get_driver()

    t0 = time()

    while time() - t0 < 30:  # wait max 30 seconds for it
        txn = await driver.begin()
        count = 0
        async for key in txn.keys(
            match=KB_RESOURCE_SLUG_BASE.format(kbid=knowledgebox), count=-1
        ):
            count += 1

        await txn.abort()
        if count == amount:
            break
        print(f"got {count}, retrying")
        await asyncio.sleep(2)

    # Add entities
    storage = await get_storage()
    txn = await driver.begin()
    kb = KnowledgeBox(txn, storage, kbid=knowledgebox, cache=None)
    entities = EntitiesGroup()
    entities.entities["entity1"].value = "PERSON"
    await kb.set_entities_force("group1", entities)

    # Add ontology
    labelset = LabelSet()
    labelset.title = "ls1"
    label = Label()
    label_title = "label1"
    label.title = label_title
    labelset.labels.append(label)
    await kb.set_labelset(label_title, labelset)
    await txn.commit(resource=False)

    yield knowledgebox
