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

import aiohttp
import pytest
from grpc import aio
from nucliadb_protos.knowledgebox_pb2 import EntitiesGroup, Label, LabelSet
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
    Position,
    Sentence,
)
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    SetEntitiesRequest,
    SetLabelsRequest,
)
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.ingest.orm.entities import EntitiesManager
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG_BASE
from nucliadb.standalone.settings import Settings
from nucliadb.train.utils import start_shard_manager, stop_shard_manager
from nucliadb_utils.tests import free_port
from nucliadb_utils.utilities import clear_global_cache, get_storage


@pytest.fixture(scope="function")
async def train_rest_api(nucliadb: Settings):  # type: ignore
    async with aiohttp.ClientSession(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://localhost:{nucliadb.http_port}",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def writer_rest_api(nucliadb: Settings):  # type: ignore
    async with aiohttp.ClientSession(
        headers={"X-NUCLIADB-ROLES": "WRITER"},
        base_url=f"http://localhost:{nucliadb.http_port}",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def knowledgebox_with_labels(nucliadb_grpc: WriterStub, knowledgebox: str):
    slr = SetLabelsRequest()
    slr.kb.uuid = knowledgebox
    slr.id = "labelset_paragraphs"
    slr.labelset.kind.append(LabelSet.LabelSetKind.PARAGRAPHS)
    l1 = Label(title="label_machine")
    l2 = Label(title="label_user")
    slr.labelset.labels.append(l1)
    slr.labelset.labels.append(l2)
    await nucliadb_grpc.SetLabels(slr)  # type: ignore

    slr = SetLabelsRequest()
    slr.kb.uuid = knowledgebox
    slr.id = "labelset_resources"
    slr.labelset.kind.append(LabelSet.LabelSetKind.RESOURCES)
    l1 = Label(title="label_machine")
    l2 = Label(title="label_user")
    slr.labelset.labels.append(l1)
    slr.labelset.labels.append(l2)
    await nucliadb_grpc.SetLabels(slr)  # type: ignore

    yield knowledgebox


@pytest.fixture(scope="function")
async def knowledgebox_with_entities(nucliadb_grpc: WriterStub, knowledgebox: str):
    ser = SetEntitiesRequest()
    ser.kb.uuid = knowledgebox
    ser.group = "PERSON"
    ser.entities.title = "PERSON"
    ser.entities.entities["Ramon"].value = "Ramon"
    ser.entities.entities["Eudald Camprubi"].value = "Eudald Camprubi"
    ser.entities.entities["Carmen Iniesta"].value = "Carmen Iniesta"
    ser.entities.entities["el Super Fran"].value = "el Super Fran"
    await nucliadb_grpc.SetEntities(ser)  # type: ignore

    ser = SetEntitiesRequest()
    ser.kb.uuid = knowledgebox
    ser.group = "ORG"
    ser.entities.title = "ORG"
    ser.entities.entities["Nuclia"].value = "Nuclia"
    ser.entities.entities["Debian"].value = "Debian"
    ser.entities.entities["Generalitat de Catalunya"].value = "Generalitat de Catalunya"
    await nucliadb_grpc.SetEntities(ser)  # type: ignore

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

    # Add a ner with positions
    fcmw.metadata.metadata.ner.update(
        {
            "Barcelona": "CITY",
            "Manresa": "CITY",
        }
    )
    fcmw.metadata.metadata.positions["CITY/Barcelona"].entity = "Barcelona"
    fcmw.metadata.metadata.positions["CITY/Barcelona"].position.append(
        Position(start=43, end=52)
    )
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
async def test_pagination_resources(
    processor: Processor, knowledgebox_ingest, test_settings_train
):
    """
    Create a set of resources with only basic information to test pagination
    """
    amount = 10

    # Create resources
    for i in range(1, amount + 1):
        message = broker_simple_resource(knowledgebox_ingest, i)
        await processor.process(message=message, seqid=-1, transaction_check=False)

        message = broker_processed_resource(knowledgebox_ingest, i, message.uuid)
        await processor.process(message=message, seqid=-1, transaction_check=False)
        # Give processed data some time to reach the node

    from time import time

    from nucliadb.common.maindb.utils import get_driver

    driver = get_driver()

    t0 = time()

    while time() - t0 < 30:  # wait max 30 seconds for it
        txn = await driver.begin()
        count = 0
        async for key in txn.keys(
            match=KB_RESOURCE_SLUG_BASE.format(kbid=knowledgebox_ingest), count=-1
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
    kb = KnowledgeBox(txn, storage, kbid=knowledgebox_ingest)
    entities_manager = EntitiesManager(kb, txn)
    entities = EntitiesGroup()
    entities.entities["entity1"].value = "PERSON"
    await entities_manager.set_entities_group_force("group1", entities)

    # Add ontology
    labelset = LabelSet()
    labelset.title = "ls1"
    label = Label()
    label_title = "label1"
    label.title = label_title
    labelset.labels.append(label)
    await kb.set_labelset(label_title, labelset)
    await txn.commit()

    yield knowledgebox_ingest


@pytest.fixture(scope="function")
def test_settings_train(cache, gcs, fake_node, maindb_driver):  # type: ignore
    from nucliadb.train.settings import settings
    from nucliadb_utils.settings import (
        FileBackendConfig,
        running_settings,
        storage_settings,
    )

    running_settings.debug = False
    print(f"Redis ready at {maindb_driver.url}")

    old_file_backend = storage_settings.file_backend
    old_gcs_endpoint_url = storage_settings.gcs_endpoint_url
    old_gcs_bucket = storage_settings.gcs_bucket
    old_grpc_port = settings.grpc_port

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = FileBackendConfig.GCS
    storage_settings.gcs_bucket = "test_{kbid}"
    settings.grpc_port = free_port()
    yield
    storage_settings.file_backend = old_file_backend
    storage_settings.gcs_endpoint_url = old_gcs_endpoint_url
    storage_settings.gcs_bucket = old_gcs_bucket
    settings.grpc_port = old_grpc_port


@pytest.fixture(scope="function")
async def train_api(test_settings_train: None, local_files):  # type: ignore
    from nucliadb.train.utils import start_train_grpc, stop_train_grpc

    await start_shard_manager()
    await start_train_grpc("testing_train")
    yield
    await stop_train_grpc()
    await stop_shard_manager()


@pytest.fixture(scope="function")
async def train_client(train_api):  # type: ignore
    from nucliadb_protos.train_pb2_grpc import TrainStub

    from nucliadb.train.settings import settings

    channel = aio.insecure_channel(f"localhost:{settings.grpc_port}")
    yield TrainStub(channel)
    clear_global_cache()
