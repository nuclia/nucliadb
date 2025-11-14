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
import logging
import random
import time
import uuid
from typing import AsyncIterator

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.ids import ParagraphId
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.resource import Resource
from nucliadb.tests.vectors import V1
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX, RESOURCES_PREFIX
from nucliadb_models.metadata import RelationEntity, RelationNodeType, RelationType
from nucliadb_protos import resources_pb2
from nucliadb_protos import utils_pb2 as upb
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.utils_pb2 import Relation, RelationMetadata, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage, FieldComputedMetadataWrapper
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def knowledgebox(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: KBShardManager,
) -> AsyncIterator[str]:
    """Knowledgebox created through the ORM. This is what ingest gRPC ends up
    calling when backend creates a hosted KB and what the standalone API ends up
    calling for onprem KBs.

    """
    kbid = KnowledgeBox.new_unique_kbid()
    kbslug = "slug-" + str(uuid.uuid4())
    model = SemanticModelMetadata(
        similarity_function=upb.VectorSimilarity.COSINE, vector_dimension=len(V1)
    )
    await KnowledgeBox.create(
        maindb_driver, kbid=kbid, slug=kbslug, semantic_models={"my-semantic-model": model}
    )

    yield kbid

    await KnowledgeBox.delete(maindb_driver, kbid)
    # await KnowledgeBox.purge(maindb_driver, kbid)


@pytest.fixture(scope="function")
async def standalone_knowledgebox(nucliadb_writer_manager: AsyncClient) -> AsyncIterator[str]:
    """Knowledgebox created through the /kbs endpoint, only accessible for
    onprem (standalone) deployments.

    This fixture requires the test using it to be decorated with a
    @pytest.mark.deploy_modes(...)

    """
    resp = await nucliadb_writer_manager.post(
        f"/{KBS_PREFIX}",
        json={
            "title": "Standalone test KB",
            "slug": "knowledgebox",
        },
    )
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    yield kbid

    resp = await nucliadb_writer_manager.delete(f"/{KB_PREFIX}/{kbid}")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
async def full_resource(
    storage: Storage,
    maindb_driver: Driver,
    knowledgebox: str,
) -> AsyncIterator[Resource]:
    """Create a resource in `knowledgebox` that has every possible bit of information.

    DISCLAIMER: this comes from a really old fixture, so probably it does
    **not** contains every possible bit of information.

    """
    from tests.ndbfixtures.ingest import create_resource

    resource = await create_resource(
        storage=storage,
        driver=maindb_driver,
        knowledgebox=knowledgebox,
    )
    yield resource
    resource.clean()


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
    maindb_driver: Driver, processor: Processor, knowledgebox: str
) -> AsyncIterator[tuple[str, list[str]]]:
    """Create a set of resources with basic information on `knowledgebox`."""
    total = 10
    resource_ids = []

    for i in range(1, total + 1):
        slug = f"simple-resource-{i}"
        bmb = BrokerMessageBuilder(kbid=knowledgebox, slug=slug)
        bmb.with_title(f"My simple resource {i}")
        bmb.with_summary(f"Summary of my simple resource {i}")
        bm = bmb.build()
        await processor.process(message=bm, seqid=i)
        resource_ids.append(bm.uuid)

    # Give processed data some time to be processed
    timeout = 5
    start = time.time()
    created_count = 0
    while created_count < total or (time.time() - start) < timeout:
        created_count = len(
            [rid async for rid in datamanagers.resources.iterate_resource_ids(kbid=knowledgebox)]
        )
        await asyncio.sleep(0.1)

    yield knowledgebox, resource_ids


# Only supported for standalone (as it depends on standalone_knowledgebox fixture)
@pytest.fixture(scope="function")
async def knowledge_graph(
    nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub, standalone_knowledgebox: str
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "summary": "Test knowledge graph",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    nodes = {
        "Animal": RelationNode(value="Animal", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Batman": RelationNode(value="Batman", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Becquer": RelationNode(value="Becquer", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Cat": RelationNode(value="Cat", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Catwoman": RelationNode(value="Catwoman", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Eric": RelationNode(value="Eric", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Fly": RelationNode(value="Fly", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Gravity": RelationNode(value="Gravity", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Joan Antoni": RelationNode(value="Joan Antoni", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Joker": RelationNode(value="Joker", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Newton": RelationNode(value="Newton", ntype=RelationNode.NodeType.ENTITY, subtype="science"),
        "Isaac Newsome": RelationNode(
            value="Isaac Newsome", ntype=RelationNode.NodeType.ENTITY, subtype="science"
        ),
        "Physics": RelationNode(value="Physics", ntype=RelationNode.NodeType.ENTITY, subtype="science"),
        "Poetry": RelationNode(value="Poetry", ntype=RelationNode.NodeType.ENTITY, subtype=""),
        "Swallow": RelationNode(value="Swallow", ntype=RelationNode.NodeType.ENTITY, subtype=""),
    }

    edges = [
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Batman"],
            to=nodes["Catwoman"],
            relation_label="love",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Batman"],
            to=nodes["Joker"],
            relation_label="fight",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Joker"],
            to=nodes["Physics"],
            relation_label="enjoy",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Catwoman"],
            to=nodes["Cat"],
            relation_label="imitate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Cat"],
            to=nodes["Animal"],
            relation_label="species",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Newton"],
            to=nodes["Physics"],
            relation_label="study",
            metadata=RelationMetadata(
                paragraph_id="myresource/0/myresource/100-200",
                source_start=1,
                source_end=10,
                to_start=11,
                to_end=20,
                data_augmentation_task_id="mytask",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Newton"],
            to=nodes["Gravity"],
            relation_label="formulate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Isaac Newsome"],
            to=nodes["Physics"],
            relation_label="study",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Isaac Newsome"],
            to=nodes["Gravity"],
            relation_label="formulate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Eric"],
            to=nodes["Cat"],
            relation_label="like",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Eric"],
            to=nodes["Joan Antoni"],
            relation_label="collaborate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Joan Antoni"],
            to=nodes["Eric"],
            relation_label="collaborate",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Joan Antoni"],
            to=nodes["Becquer"],
            relation_label="read",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Becquer"],
            to=nodes["Poetry"],
            relation_label="write",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Becquer"],
            to=nodes["Poetry"],
            relation_label="like",
        ),
        Relation(
            relation=Relation.RelationType.ABOUT,
            source=nodes["Poetry"],
            to=nodes["Swallow"],
            relation_label="about",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Swallow"],
            to=nodes["Animal"],
            relation_label="species",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Swallow"],
            to=nodes["Fly"],
            relation_label="can",
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["Fly"],
            to=nodes["Gravity"],
            relation_label="defy",
        ),
    ]

    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = standalone_knowledgebox
    bm.user_relations.relations.extend(edges)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/entitiesgroups",
        json={
            "title": "scientist",
            "color": "",
            "entities": {
                "Isaac": {"value": "Isaac"},
                "Isaac Newton": {"value": "Isaac Newton", "represents": ["Newton"]},
                "Isaac Newsome": {"value": "Isaac Newsome"},
            },
            "custom": True,
            "group": "scientist",
        },
    )
    assert resp.status_code == 200, resp.content
    resp = await nucliadb_writer.patch(
        f"/kb/{standalone_knowledgebox}/entitiesgroup/scientist",
        json={"add": {}, "update": {}, "delete": ["Isaac Newsome"]},
    )
    assert resp.status_code == 200, resp.content
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/entitiesgroups",
        json={
            "title": "poet",
            "color": "",
            "entities": {
                "Becquer": {
                    "value": "Becquer",
                    "represents": ["Gustavo Adolfo Bécquer"],
                },
                "Gustavo Adolfo Bécquer": {"value": "Gustavo Adolfo Bécquer"},
            },
            "custom": True,
            "group": "poet",
        },
    )
    assert resp.status_code == 200, resp.content

    return (nodes, edges, rid)


@pytest.fixture(scope="function")
async def kb_with_entity_graph(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    entity_graph: tuple[dict[str, RelationEntity], dict[str, RelationType], list[tuple[str, str, str]]],
) -> AsyncIterator[str]:
    kbid = standalone_knowledgebox
    entities, relations, paths = entity_graph

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "usermetadata": {
                "relations": [
                    {
                        "relation": relations[relation].value,
                        "label": relation,
                        "from": entities[source].model_dump(),
                        "to": entities[target].model_dump(),
                    }
                    for source, relation, target in paths
                ],
            },
        },
    )
    assert resp.status_code == 201

    yield kbid


@pytest.fixture(scope="function")
def entity_graph() -> tuple[
    dict[str, RelationEntity], dict[str, RelationType], list[tuple[str, str, str]]
]:
    entities = {
        "Anastasia": RelationEntity(value="Anastasia", type=RelationNodeType.ENTITY, group="PERSON"),
        "Anna": RelationEntity(value="Anna", type=RelationNodeType.ENTITY, group="PERSON"),
        "Apollo": RelationEntity(value="Apollo", type=RelationNodeType.ENTITY, group="PROJECT"),
        "Cat": RelationEntity(value="Cat", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "Climbing": RelationEntity(value="Climbing", type=RelationNodeType.ENTITY, group="ACTIVITY"),
        "Computer science": RelationEntity(
            value="Computer science", type=RelationNodeType.ENTITY, group="STUDY_FIELD"
        ),
        "Dimitri": RelationEntity(value="Dimitri", type=RelationNodeType.ENTITY, group="PERSON"),
        "Erin": RelationEntity(value="Erin", type=RelationNodeType.ENTITY, group="PERSON"),
        "Jerry": RelationEntity(value="Jerry", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "Margaret": RelationEntity(value="Margaret", type=RelationNodeType.ENTITY, group="PERSON"),
        "Mr. P": RelationEntity(value="Mr. P", type=RelationNodeType.ENTITY, group="AGENT"),
        "Mouse": RelationEntity(value="Mouse", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "New York": RelationEntity(value="New York", type=RelationNodeType.ENTITY, group="PLACE"),
        "Olympic athlete": RelationEntity(
            value="Olympic athlete", type=RelationNodeType.ENTITY, group="SPORT"
        ),
        "Peter": RelationEntity(value="Peter", type=RelationNodeType.ENTITY, group="PERSON"),
        "Rocket": RelationEntity(value="Rocket", type=RelationNodeType.ENTITY, group="VEHICLE"),
        "Tom": RelationEntity(value="Tom", type=RelationNodeType.ENTITY, group="ANIMAL"),
        "UK": RelationEntity(value="UK", type=RelationNodeType.ENTITY, group="PLACE"),
    }
    relations = {
        "ALIAS": RelationType.SYNONYM,
        "BORN_IN": RelationType.ENTITY,
        "CHASE": RelationType.ENTITY,
        "DEVELOPED": RelationType.ENTITY,
        "FOLLOW": RelationType.ENTITY,
        "IS": RelationType.ENTITY,
        "IS_FRIEND": RelationType.ENTITY,
        "LIVE_IN": RelationType.ENTITY,
        "LOVE": RelationType.ENTITY,
        "WORK_IN": RelationType.ENTITY,
    }
    graph = [
        ("Anastasia", "IS_FRIEND", "Anna"),
        ("Anna", "FOLLOW", "Erin"),
        ("Anna", "LIVE_IN", "New York"),
        ("Anna", "LOVE", "Cat"),
        ("Anna", "WORK_IN", "New York"),
        ("Apollo", "IS", "Rocket"),
        ("Dimitri", "LOVE", "Anastasia"),
        ("Erin", "BORN_IN", "UK"),
        ("Erin", "IS", "Olympic athlete"),
        ("Erin", "LOVE", "Climbing"),
        ("Jerry", "IS", "Mouse"),
        ("Margaret", "DEVELOPED", "Apollo"),
        ("Margaret", "WORK_IN", "Computer science"),
        ("Mr. P", "ALIAS", "Peter"),
        ("Peter", "LIVE_IN", "New York"),
        ("Tom", "CHASE", "Jerry"),
        ("Tom", "IS", "Cat"),
    ]

    return (entities, relations, graph)


@pytest.fixture(scope="function")
async def graph_resource(nucliadb_writer: AsyncClient, nucliadb_ingest_grpc, standalone_knowledgebox):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "summary": "Test knowledge graph",
            "texts": {
                "inception1": {"body": "Christopher Nolan directed Inception. Very interesting movie."},
                "inception2": {"body": "Leonardo DiCaprio starred in Inception."},
                "inception3": {"body": "Joseph Gordon-Levitt starred in Inception."},
                "leo": {"body": "Leonardo DiCaprio is a great actor. DiCaprio started acting in 1989."},
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    nodes = {
        "nolan": RelationNode(
            value="Christopher Nolan", ntype=RelationNode.NodeType.ENTITY, subtype="DIRECTOR"
        ),
        "inception": RelationNode(
            value="Inception", ntype=RelationNode.NodeType.ENTITY, subtype="MOVIE"
        ),
        "leo": RelationNode(
            value="Leonardo DiCaprio", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"
        ),
        "dicaprio": RelationNode(value="DiCaprio", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"),
        "levitt": RelationNode(
            value="Joseph Gordon-Levitt", ntype=RelationNode.NodeType.ENTITY, subtype="ACTOR"
        ),
    }

    edges = [
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["nolan"],
            to=nodes["inception"],
            relation_label="directed",
            metadata=RelationMetadata(
                # Set this field id as int enum value since this is how legacy relations reported paragraph_id
                paragraph_id=rid + "/4/inception1/0-37",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["leo"],
            to=nodes["inception"],
            relation_label="starred",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/inception2/0-39",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["levitt"],
            to=nodes["inception"],
            relation_label="starred",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/inception3/0-42",
            ),
        ),
        Relation(
            relation=Relation.RelationType.ENTITY,
            source=nodes["leo"],
            to=nodes["dicaprio"],
            relation_label="analogy",
            metadata=RelationMetadata(
                paragraph_id=rid + "/t/leo/0-70",
                data_augmentation_task_id="my_graph_task_id",
            ),
        ),
    ]

    # Add relations to the resource as processor does
    for n in nodes.values():
        edges.append(
            Relation(
                relation=Relation.RelationType.ENTITY,
                source=RelationNode(value=rid, ntype=RelationNode.NodeType.RESOURCE),
                to=n,
            )
        )

    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = standalone_knowledgebox
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "inception1"
    fcmw.metadata.metadata.relations.add(relations=edges)
    bm.field_metadata.append(fcmw)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()
    yield rid


async def smb_wonder_resource(
    kbid: str, nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub
) -> str:
    """Resource with a PDF file field, extracted vectors and question and
    answers.

    """
    slug = "smb-wonder"
    field_id = "smb-wonder"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "slug": slug,
            "title": "SMB Wonder",
            "files": {
                field_id: {
                    "language": "en",
                    "file": {
                        "filename": "smb_wonder.pdf",
                        "content_type": "application/pdf",
                        "payload": base64.b64encode(b"Super Mario Wonder: the game").decode(),
                    },
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
    random.seed(63)

    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        slug=slug,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    bmb.with_title("Super Mario Bros. Wonder")
    bmb.with_summary("SMB Wonder: the new Mario game from Nintendo")

    field_builder = bmb.field_builder(field_id, FieldType.FILE)
    # ids are hardcoded so it's easier to copy while testing and changes on them may break tests
    paragraphs = [
        (
            ParagraphId.from_string(f"{rid}/f/smb-wonder/0-99"),
            "Super Mario Bros. Wonder (SMB Wonder) is a 2023 platform game developed and published by Nintendo.\n",  # noqa
        ),
        (
            ParagraphId.from_string(f"{rid}/f/smb-wonder/99-145"),
            "SMB Wonder is a side-scrolling plaftorm game.\n",
        ),
        (
            ParagraphId.from_string(f"{rid}/f/smb-wonder/145-234"),
            "As one of eight player characters, the player completes levels across the Flower Kingdom.",  # noqa
        ),
    ]
    for expected_paragraph_id, paragraph in paragraphs:
        paragraph_id, _ = field_builder.add_paragraph(
            paragraph,
            vectors={
                vectorset_id: [
                    random.random() for _ in range(config.vectorset_index_config.vector_dimension)
                ]
                for i, (vectorset_id, config) in enumerate(vectorsets.items())
            },
        )
        assert paragraph_id == expected_paragraph_id

    # add Q&A

    question = "What is SMB Wonder?"
    field_builder.add_question_answer(
        question=question,
        question_paragraph_ids=[paragraphs[0][0].full()],
        answer="SMB Wonder is a side-scrolling Nintendo Switch game",
        answer_paragraph_ids=[paragraphs[0][0].full(), paragraphs[1][0].full()],
    )
    field_builder.add_question_answer(
        question=question,
        question_paragraph_ids=[paragraphs[0][0].full()],
        answer="It's the new Mario game for Nintendo Switch",
        answer_paragraph_ids=[paragraphs[0][0].full()],
    )

    question = "Give me an example of side-scrolling game"
    field_builder.add_question_answer(
        question=question,
        answer="SMB Wonder game",
        answer_paragraph_ids=[paragraphs[1][0].full()],
    )

    bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid


async def cookie_tale_resource(
    kbid: str, nucliadb_writer: AsyncClient, nucliadb_ingest_grpc: WriterStub
) -> str:
    """Resource with metadata (origin and security), a text field and a file
    field with pages, visual content and tables. Fields have extracted vectors.

    """
    slug = "cookie-tale"
    text_field_id = "cookie-tale"
    file_field_id = "cookie-recipie"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{RESOURCES_PREFIX}",
        json={
            "slug": slug,
            "title": "Replaced title",
            "summary": "Replaced summary",
            "origin": {
                "url": "my://url",
            },
            "security": {
                "access_groups": [
                    "developers",
                    "testers",
                ]
            },
            "texts": {
                text_field_id: {
                    "body": "Replaced text",
                    "format": "PLAIN",
                },
            },
            "files": {
                file_field_id: {
                    "language": "en",
                    "file": {
                        "filename": "cookies.pdf",
                        "content_type": "application/pdf",
                        "payload": base64.b64encode(b"some content we're not going to check").decode(),
                    },
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    storage = await get_storage()

    vectorsets = {}
    async with datamanagers.with_ro_transaction() as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            vectorsets[vectorset_id] = vs
    # use a controlled random seed for vector generation
    random.seed(63)

    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        slug=slug,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )

    title_field = bmb.with_title("A tale of cookies")
    bmb.with_summary("Once upon a time, cookies were made...")

    ## Add a text field with paragraphs and paragraph relations

    text_field = bmb.field_builder(text_field_id, FieldType.TEXT)
    extracted_text = [
        "Once upon a time, there was a group of people called Nucliers. ",
        "One of them was an excellent cook and use to bring amazing cookies to their gatherings. ",
        "Chocolate, peanut butter and other delicious kinds of cookies. ",
        "Everyone loved them and those cookies are now part of their story. ",
    ]
    paragraph_ids = []
    paragraph_pbs = []
    for paragraph in extracted_text:
        paragraph_id, paragraph_pb = text_field.add_paragraph(
            paragraph,
            vectors={
                vectorset_id: [
                    random.random() for _ in range(config.vectorset_index_config.vector_dimension)
                ]
                for i, (vectorset_id, config) in enumerate(vectorsets.items())
            },
        )
        paragraph_ids.append(paragraph_id)
        paragraph_pbs.append(paragraph_pb)

    # add paragraph relations

    title_paragraph_id = list(title_field.iter_paragraphs())[0][0]
    paragraph_pbs[1].relations.parents.append(title_paragraph_id.full())

    paragraph_pbs[1].relations.siblings.append(paragraph_ids[0].full())

    paragraph_pbs[1].relations.replacements.extend([paragraph_ids[2].full(), paragraph_ids[3].full()])

    ## Add a file field with some visual content, pages and a table

    file_field = bmb.field_builder(file_field_id, FieldType.FILE)

    (_, paragraph_pb) = file_field.add_paragraph(
        "A yummy image of some cookies",
        kind=resources_pb2.Paragraph.TypeParagraph.INCEPTION,
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )
    paragraph_pb.representation.reference_file = "cookies.png"

    # upload an source "image" for this paragraph
    sf = storage.file_extracted(bmb.bm.kbid, bmb.bm.uuid, "f", file_field_id, "generated/cookies.png")
    await storage.chunked_upload_object(sf.bucket, sf.key, payload=b"delicious cookies image")

    paragraph_pb.page.page = 0
    paragraph_pb.page.page_with_visual = True
    await file_field.add_page_preview(
        page=0,
        content=b"A page with an image of cookies",
    )

    # add a table.

    (_, paragraph_pb) = file_field.add_paragraph(
        "|Ingredient|Quantity|\n|Peanut butter|100g|\n...",
        kind=resources_pb2.Paragraph.TypeParagraph.TABLE,
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )
    paragraph_pb.representation.is_a_table = True
    paragraph_pb.representation.reference_file = "ingredients_table.png"

    # unused right now, but this would be the source image for the table
    sf = storage.file_extracted(
        bmb.bm.kbid, bmb.bm.uuid, "f", file_field_id, "generated/ingredients_table.png"
    )
    await storage.chunked_upload_object(sf.bucket, sf.key, payload=b"ingredients table")

    paragraph_pb.page.page = 1
    paragraph_pb.page.page_with_visual = True
    await file_field.add_page_preview(
        page=1,
        content=b"A page with a table with ingredients and quantities",
    )

    # add a normal paragraph in the same page
    (_, paragraph_pb) = file_field.add_paragraph(
        "Above you can see a table with all the ingredients",
        vectors={
            vectorset_id: [
                random.random() for _ in range(config.vectorset_index_config.vector_dimension)
            ]
            for i, (vectorset_id, config) in enumerate(vectorsets.items())
        },
    )

    paragraph_pb.page.page = 1
    paragraph_pb.page.page_with_visual = True

    bm = bmb.build()

    # customize fields we don't want to overwrite from the writer BM
    bm.origin.url = "my://url"

    # ingest the processed BM
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid
