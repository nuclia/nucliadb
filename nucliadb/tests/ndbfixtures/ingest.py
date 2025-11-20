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
import logging
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from os.path import dirname, getsize
from typing import AsyncIterator, Iterable, Optional
from unittest.mock import AsyncMock, patch

import nats
import pytest
from grpc import aio

from nucliadb.common import datamanagers
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.consumer import service as consumer_service
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.service.writer import WriterServicer
from nucliadb.standalone.settings import Settings
from nucliadb.tests.vectors import V1, V2, V3
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos import utils_pb2 as upb
from nucliadb_protos import writer_pb2_grpc
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata, VectorSetConfig
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import nuclia_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.transaction import TransactionUtility
from nucliadb_utils.utilities import (
    clear_global_cache,
)

logger = logging.getLogger(__name__)

# This is used to compute paths to assets used in different tests. Before, we
# were doing an os.path.dirname(__file__) on the ingest/fixtures.py, but this is
# more portable
INGEST_TESTS_DIR = os.path.abspath(os.path.join(dirname(__file__), "..", "ingest"))


@dataclass
class IngestGrpcServer:
    host: str
    port: int

    @property
    def address(self):
        return f"{self.host}:{self.port}"


# Main fixtures


@pytest.fixture(scope="function")
async def component_nucliadb_ingest_grpc(
    ingest_grpc_server: IngestGrpcServer,
) -> AsyncIterator[WriterStub]:
    channel = aio.insecure_channel(ingest_grpc_server.address)
    stub = WriterStub(channel)
    yield stub  # type: ignore
    await channel.close(grace=None)


@pytest.fixture(scope="function")
async def standalone_nucliadb_ingest_grpc(nucliadb: Settings) -> AsyncIterator[WriterStub]:
    channel = aio.insecure_channel(f"localhost:{nucliadb.ingest_grpc_port}")
    stub = WriterStub(channel)
    yield stub  # type: ignore
    await channel.close(grace=None)


# Utils


@pytest.fixture(scope="function")
async def ingest_grpc_server(
    maindb_driver: Driver,
    storage: Storage,
    shard_manager: KBShardManager,
) -> AsyncIterator[IngestGrpcServer]:
    """Ingest ORM gRPC server with dummy/mocked index."""
    servicer = WriterServicer()
    await servicer.initialize()
    server = aio.server()
    port = server.add_insecure_port("[::]:0")
    writer_pb2_grpc.add_WriterServicer_to_server(servicer, server)
    await server.start()
    yield IngestGrpcServer(
        host="127.0.0.1",
        port=port,
    )
    await servicer.finalize()
    await server.stop(None)


# TODO: keep cleaning this fixtures coming from ingest/fixtures.py


@pytest.fixture(scope="function")
def processor(maindb_driver: Driver, storage: Storage, pubsub: PubSubDriver) -> Iterable[Processor]:
    proc = Processor(maindb_driver, storage, pubsub, partition="1")
    yield proc


@dataclass
class IngestFixture:
    servicer: WriterServicer
    channel: aio.Channel
    host: str
    serv: aio.Server


@pytest.fixture(scope="function")
async def ingest_consumers(
    maindb_driver: Driver,
    transaction_utility: TransactionUtility,
    storage: Storage,
    dummy_nidx_utility,
    nats_manager: NatsConnectionManager,
    nats_ingest_stream,
):
    ingest_consumers_finalizer = await consumer_service.start_ingest_consumers()

    yield

    await ingest_consumers_finalizer()
    clear_global_cache()


@pytest.fixture(scope="function")
async def ingest_processed_consumer(
    maindb_driver: Driver,
    transaction_utility: TransactionUtility,
    storage: Storage,
    dummy_nidx_utility,
    nats_manager: NatsConnectionManager,
    nats_ingest_processed_stream,
):
    ingest_consumer_finalizer = await consumer_service.start_ingest_processed_consumer_v2()

    yield

    await ingest_consumer_finalizer()
    clear_global_cache()


@pytest.fixture(scope="function")
def learning_config():
    with patch.object(nuclia_settings, "dummy_learning_services", True):
        yield


@pytest.fixture(scope="function")
async def knowledgebox_with_vectorsets(
    storage: Storage, maindb_driver: Driver, shard_manager, learning_config
):
    kbid = KnowledgeBox.new_unique_kbid()
    kbslug = "slug-" + str(uuid.uuid4())
    await KnowledgeBox.create(
        maindb_driver,
        kbid=kbid,
        slug=kbslug,
        semantic_models={
            "my-semantic-model-A": SemanticModelMetadata(
                similarity_function=upb.VectorSimilarity.COSINE,
                vector_dimension=len(V1),
            ),
            "my-semantic-model-B": SemanticModelMetadata(
                similarity_function=upb.VectorSimilarity.COSINE,
                vector_dimension=512,
                matryoshka_dimensions=[3072, 512, 128],
            ),
            "my-semantic-model-C": SemanticModelMetadata(
                similarity_function=upb.VectorSimilarity.DOT,
                vector_dimension=1024,
            ),
        },
    )

    yield kbid

    await KnowledgeBox.delete(maindb_driver, kbid)


@pytest.fixture(scope="function")
async def nats_ingest_stream(nats_server: str):
    streams = [
        (const.Streams.INGEST.name, const.Streams.INGEST.subject.format(partition=">")),
    ]
    consumers = [
        (const.Streams.INGEST.name, const.Streams.INGEST.group.format(partition="1")),
    ]
    async with _nats_streams_and_consumers_setup(nats_server, streams, consumers):
        yield


@pytest.fixture(scope="function")
async def nats_ingest_processed_stream(nats_server: str):
    streams = [
        (const.Streams.INGEST.name, const.Streams.INGEST.subject.format(partition=">")),
    ]
    async with _nats_streams_and_consumers_setup(nats_server, streams, []):
        yield


@asynccontextmanager
async def _nats_streams_and_consumers_setup(
    nats_server: str,
    streams: list[tuple[str, str]],
    consumers: list[tuple[str, str]],
    clean: bool = True,
):
    nc = await nats.connect(servers=[nats_server])
    js = nc.jetstream()

    # create streams
    for stream, subject in streams:
        try:
            await js.stream_info(name=stream)
        except nats.js.errors.NotFoundError:
            await js.add_stream(name=stream, subjects=[subject])

    await nc.close()

    yield

    # we close and reopen the connection with nats between yield points to avoid
    # warnings complaining on tasks being closed and tasks being killed.
    # Probably some nats invariant is breaking across yield points
    if clean:
        nc = await nats.connect(servers=[nats_server])
        js = nc.jetstream()

        # delete consumers
        for stream, consumer in consumers:
            try:
                await js.delete_consumer(stream, consumer)
            except nats.js.errors.NotFoundError:
                pass

        # delete streams
        for stream, subject in streams:
            try:
                await js.delete_stream(stream)
            except nats.js.errors.NotFoundError:
                pass

        await nc.close()


THUMBNAIL = rpb.CloudFile(
    uri="thumbnail.png",
    source=rpb.CloudFile.Source.LOCAL,
    bucket_name="/integration/orm/assets",
    size=getsize(f"{INGEST_TESTS_DIR}/integration/orm/assets/thumbnail.png"),
    content_type="image/png",
    filename="thumbnail.png",
)

TEST_CLOUDFILE_FILENAME = "text.pb"
TEST_CLOUDFILE = rpb.CloudFile(
    uri=TEST_CLOUDFILE_FILENAME,
    source=rpb.CloudFile.Source.LOCAL,
    bucket_name="/integration/orm/assets",
    size=getsize(f"{INGEST_TESTS_DIR}/integration/orm/assets/{TEST_CLOUDFILE_FILENAME}"),
    content_type="application/octet-stream",
    filename=TEST_CLOUDFILE_FILENAME,
    md5="01cca3f53edb934a445a3112c6caa652",
)


# HELPERS
async def kb_vectorsets(kb: KnowledgeBox) -> list[VectorSetConfig]:
    vectorsets = []
    async for _, vs in datamanagers.vectorsets.iter(kb.txn, kbid=kb.kbid):
        vectorsets.append(vs)
    return vectorsets


async def make_field(field: Field, extracted_text: str):
    vectorsets = await kb_vectorsets(field.resource.kb)
    await field.set_extracted_text(make_extracted_text(field.id, body=extracted_text))
    await field.set_field_metadata(make_field_metadata(field.id))
    await field.set_large_field_metadata(make_field_large_metadata(field.id))
    assert len(vectorsets) > 0, "KBs must have at least a vectorset"
    for idx, vs in enumerate(vectorsets):
        await field.set_vectors(
            make_extracted_vectors(field.id, vs, idx),
            vectorset=vs.vectorset_id,
            storage_key_kind=vs.storage_key_kind,
        )


def make_extracted_text(field_id: str, body: str) -> rpb.ExtractedTextWrapper:
    ex1 = rpb.ExtractedTextWrapper()
    ex1.field.CopyFrom(rpb.FieldID(field_type=rpb.FieldType.TEXT, field=field_id))
    ex1.body.text = body
    return ex1


def make_field_metadata(field_id: str) -> rpb.FieldComputedMetadataWrapper:
    ex1 = rpb.FieldComputedMetadataWrapper()
    ex1.field.CopyFrom(rpb.FieldID(field_type=rpb.FieldType.TEXT, field=field_id))
    ex1.metadata.metadata.links.append("https://nuclia.com")

    p1 = rpb.Paragraph(start=0, end=20)
    p1.sentences.append(rpb.Sentence(start=0, end=20, key=""))
    cl1 = rpb.Classification(labelset="labelset1", label="label1")
    cl2 = rpb.Classification(labelset="paragraph-labelset", label="label1")
    p1.classifications.append(cl2)
    ex1.metadata.metadata.paragraphs.append(p1)
    ex1.metadata.metadata.classifications.append(cl1)
    # ex1.metadata.metadata.ner["Ramon"] = "PEOPLE"
    ex1.metadata.metadata.last_index.FromDatetime(datetime.now())
    ex1.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    ex1.metadata.metadata.last_extract.FromDatetime(datetime.now())
    ex1.metadata.metadata.last_summary.FromDatetime(datetime.now())
    ex1.metadata.metadata.thumbnail.CopyFrom(THUMBNAIL)
    # Data Augmentation + Processor entities
    ex1.metadata.metadata.entities["processor"].entities.extend(
        [
            rpb.FieldEntity(
                text="document",
                label="ENTITY",
                positions=[
                    rpb.Position(start=0, end=5),
                    rpb.Position(start=13, end=18),
                ],
            ),
        ]
    )
    ex1.metadata.metadata.entities["my-task-id"].entities.extend(
        [
            rpb.FieldEntity(
                text="document",
                label="NOUN",
                positions=[
                    rpb.Position(start=0, end=5),
                    rpb.Position(start=13, end=18),
                ],
            ),
        ]
    )
    ex1.metadata.metadata.mime_type = "text/html"
    return ex1


def make_field_large_metadata(field_id: str) -> rpb.LargeComputedMetadataWrapper:
    ex1 = rpb.LargeComputedMetadataWrapper()
    ex1.field.CopyFrom(rpb.FieldID(field_type=rpb.FieldType.TEXT, field=field_id))
    en1 = rpb.Entity(token="tok1", root="tok", type="NAME")
    en2 = rpb.Entity(token="tok2", root="tok2", type="NAME")
    ex1.real.metadata.entities.append(en1)
    ex1.real.metadata.entities.append(en2)
    ex1.real.metadata.tokens["tok"] = 3
    return ex1


def make_extracted_vectors(
    field_id: str, vectorset: VectorSetConfig, vectorset_idx: int
) -> rpb.ExtractedVectorsWrapper:
    ex1 = rpb.ExtractedVectorsWrapper()
    ex1.field.CopyFrom(rpb.FieldID(field_type=rpb.FieldType.TEXT, field=field_id))
    ex1.vectorset_id = vectorset.vectorset_id
    dimension = vectorset.vectorset_index_config.vector_dimension
    # We set a distinctive vector that we can later check
    v1 = rpb.Vector(start=0, end=20, vector=[float(vectorset_idx)] * dimension)
    ex1.vectors.vectors.vectors.append(v1)
    return ex1


@pytest.fixture(scope="function")
async def test_resource(storage: Storage, maindb_driver: Driver, knowledgebox: str):
    """
    Create a resource that has every possible bit of information
    """
    resource = await create_resource(
        storage=storage,
        driver=maindb_driver,
        knowledgebox=knowledgebox,
    )
    yield resource
    resource.clean()


def broker_resource(
    knowledgebox: str, rid: Optional[str] = None, slug: Optional[str] = None
) -> BrokerMessage:
    if rid is None:
        rid = str(uuid.uuid4())
    if slug is None:
        slug = f"{rid}slug1"

    message1: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=slug,
        type=BrokerMessage.AUTOCOMMIT,
    )

    message1.basic.icon = "text/plain"
    message1.basic.title = "Title Resource"
    message1.basic.summary = "Summary of document"
    message1.basic.thumbnail = "doc"
    message1.basic.metadata.useful = True
    message1.basic.metadata.language = "es"
    message1.basic.created.FromDatetime(datetime.now())
    message1.basic.modified.FromDatetime(datetime.now())
    message1.origin.source = rpb.Origin.Source.WEB

    message1.files["file"].file.uri = "http://nofile"
    message1.files["file"].file.size = 0
    message1.files["file"].file.source = rpb.CloudFile.Source.LOCAL

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer?"
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    message1.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of document"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    message1.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Title Resource"
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    message1.extracted_text.append(etw)

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    p1 = rpb.Paragraph(
        start=0,
        end=45,
    )
    p1.start_seconds.append(0)
    p1.end_seconds.append(10)
    p2 = rpb.Paragraph(
        start=47,
        end=64,
    )
    p2.start_seconds.append(10)
    p2.end_seconds.append(20)
    p2.start_seconds.append(20)
    p2.end_seconds.append(30)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.entities["processor"].entities.extend(
        [rpb.FieldEntity(text="Ramon", label="PERSON")]
    )
    fcm.metadata.metadata.entities["my-data-augmentation"].entities.extend(
        [rpb.FieldEntity(text="Ramon", label="CTO")]
    )

    c1 = rpb.Classification()
    c1.label = "label1"
    c1.labelset = "labelset1"
    fcm.metadata.metadata.classifications.append(c1)
    message1.field_metadata.append(fcm)

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

    message1.field_vectors.append(ev)
    message1.source = BrokerMessage.MessageSource.WRITER
    return message1


async def create_resource(storage: Storage, driver: Driver, knowledgebox: str) -> Resource:
    async with driver.rw_transaction() as txn:
        rid = str(uuid.uuid4())
        kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
        test_resource = await kb_obj.add_resource(uuid=rid, slug="slug")
        await test_resource.set_slug()

        # 1.  ROOT ELEMENTS
        # 1.1 BASIC

        basic = rpb.Basic(
            title="My title",
            summary="My summary",
            icon="text/plain",
            thumbnail="/file",
            last_seqid=1,
            last_account_seq=2,
        )
        basic.metadata.metadata["key"] = "value"
        basic.metadata.language = "ca"
        basic.metadata.useful = True
        basic.metadata.status = rpb.Metadata.Status.PROCESSED

        cl1 = rpb.Classification(labelset="labelset1", label="label1")
        basic.usermetadata.classifications.append(cl1)

        ufm1 = rpb.UserFieldMetadata(
            paragraphs=[rpb.ParagraphAnnotation(classifications=[cl1], key=f"{rid}/t/text1/0-20")],
            field=rpb.FieldID(field_type=rpb.FieldType.TEXT, field="text1"),
        )
        basic.fieldmetadata.append(ufm1)
        basic.created.FromDatetime(datetime.now(timezone.utc))
        basic.modified.FromDatetime(datetime.now(timezone.utc))

        await test_resource.set_basic(basic)

        # 1.2 USER RELATIONS

        rels = []
        r1 = upb.Relation(
            relation=upb.Relation.CHILD,
            source=upb.RelationNode(value=rid, ntype=upb.RelationNode.NodeType.RESOURCE),
            to=upb.RelationNode(value="000001", ntype=upb.RelationNode.NodeType.RESOURCE),
        )

        rels.append(r1)
        await test_resource.set_user_relations(rpb.Relations(relations=rels))

        # 1.3 ORIGIN

        o2 = rpb.Origin()
        o2.source = rpb.Origin.Source.API
        o2.source_id = "My Source"
        o2.created.FromDatetime(datetime.now())
        o2.modified.FromDatetime(datetime.now())

        await test_resource.set_origin(o2)

        # 2.  FIELDS
        #
        # Add an example of each of the files, containing all possible metadata

        # Title
        title_field = await test_resource.get_field("title", rpb.FieldType.GENERIC, load=False)
        await make_field(title_field, "MyText")

        # Summary
        summary_field = await test_resource.get_field("summary", rpb.FieldType.GENERIC, load=False)
        await make_field(summary_field, "MyText")

        # 2.1 FILE FIELD

        t2 = rpb.FieldFile(
            language="es",
        )
        t2.added.FromDatetime(datetime.now())
        t2.file.CopyFrom(TEST_CLOUDFILE)

        file_field = await test_resource.set_field(rpb.FieldType.FILE, "file1", t2)
        await add_field_id(test_resource, file_field)
        await make_field(file_field, "MyText")

        # 2.2 LINK FIELD
        li2 = rpb.FieldLink(
            uri="htts://nuclia.cloud",
            language="ca",
        )
        li2.added.FromDatetime(datetime.now())
        li2.headers["AUTHORIZATION"] = "Bearer xxxxx"
        linkfield = await test_resource.set_field(rpb.FieldType.LINK, "link1", li2)

        ex1 = rpb.LinkExtractedData()
        ex1.date.FromDatetime(datetime.now())
        ex1.language = "ca"
        ex1.title = "My Title"
        ex1.field = "link1"

        ex1.link_preview.CopyFrom(THUMBNAIL)
        ex1.link_thumbnail.CopyFrom(THUMBNAIL)

        await linkfield.set_link_extracted_data(ex1)
        await add_field_id(test_resource, linkfield)
        await make_field(linkfield, "MyText")

        # 2.3 TEXT FIELDS

        t23 = rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
        textfield = await test_resource.set_field(rpb.FieldType.TEXT, "text1", t23)
        await add_field_id(test_resource, textfield)
        await make_field(textfield, "MyText")

        # 2.4 CONVERSATION FIELD

        def make_message(text: str, files: Optional[list[rpb.CloudFile]] = None) -> rpb.Message:
            msg = rpb.Message(
                who="myself",
            )
            msg.timestamp.FromDatetime(datetime.now())
            msg.content.text = text
            msg.content.format = rpb.MessageContent.Format.PLAIN

            if files:
                for file in files:
                    msg.content.attachments.append(file)
            return msg

        c2 = rpb.Conversation()

        for i in range(300):
            new_message = make_message(f"{i} hello")
            if i == 33:
                new_message = make_message(f"{i} hello", files=[TEST_CLOUDFILE, THUMBNAIL])
            c2.messages.append(new_message)

        convfield = await test_resource.set_field(rpb.FieldType.CONVERSATION, "conv1", c2)
        await add_field_id(test_resource, convfield)
        await make_field(convfield, extracted_text="MyText")

        # Q/A
        field_obj = await test_resource.get_field("link1", type=rpb.FieldType.LINK)
        question_answers = rpb.FieldQuestionAnswerWrapper()
        for i in range(10):
            qa = rpb.QuestionAnswer()

            qa.question.text = f"My question {i}"
            qa.question.language = "catalan"
            qa.question.ids_paragraphs.extend([f"id1/{i}", f"id2/{i}"])

            answer = rpb.Answers()
            answer.text = f"My answer {i}"
            answer.language = "catalan"
            answer.ids_paragraphs.extend([f"id1/{i}", f"id2/{i}"])
            qa.answers.append(answer)
            question_answers.question_answers.question_answers.question_answer.append(qa)

        await field_obj.set_question_answers(question_answers)

        await txn.commit()
        return test_resource


async def add_field_id(resource: Resource, field: Field):
    field_type = FIELD_TYPE_STR_TO_PB[field.type]
    field_id = rpb.FieldID(field_type=field_type, field=field.id)
    await resource.update_all_field_ids(updated=[field_id])


@pytest.fixture(scope="function")
async def entities_manager_mock():
    """EntitiesManager mock for ingest gRPC API disabling indexed entities
    functionality. As tests doesn't startup a node, with this mock we allow
    testing ingest's gRPC API while the whole entities functionality is properly
    tested in tests nos using this fixture.

    """
    klass = "nucliadb.ingest.service.writer.EntitiesManager"
    with patch(f"{klass}.get_indexed_entities_group", AsyncMock(return_value=None)):
        with patch(
            "nucliadb.common.cluster.manager.KBShardManager.apply_for_all_shards",
            AsyncMock(return_value=[]),
        ):
            yield
