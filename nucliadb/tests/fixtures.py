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
import base64
import logging
import os
import tempfile
from unittest.mock import AsyncMock, Mock

import pytest
from grpc import aio
from httpx import AsyncClient
from pytest_docker_fixtures import images

from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.exceptions import UnsetUtility
from nucliadb.common.maindb.utils import get_driver
from nucliadb.standalone.config import config_nucliadb
from nucliadb.standalone.run import run_async_nucliadb
from nucliadb.standalone.settings import Settings
from nucliadb.tests.config import reset_config
from nucliadb.writer import API_PREFIX
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.settings import (
    LogFormatType,
    LogLevel,
    LogOutputType,
    LogSettings,
)
from nucliadb_utils.aiopynecone.models import QueryResponse
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.tests import free_port
from nucliadb_utils.utilities import (
    Utility,
    clean_pinecone,
    clean_utility,
    clear_global_cache,
    get_pinecone,
    get_utility,
    set_utility,
)
from tests.utils import inject_message
from tests.utils.dirty_index import mark_dirty, wait_for_sync

logger = logging.getLogger(__name__)

# Minimum support PostgreSQL version
# Reason: We want the btree_gin extension to support uuid's (pg11) and `gen_random_uuid()` (pg13)
images.settings["postgresql"]["version"] = "13"
images.settings["postgresql"]["env"]["POSTGRES_PASSWORD"] = "postgres"


@pytest.fixture(scope="function")
async def dummy_processing():
    from nucliadb_utils.settings import nuclia_settings

    nuclia_settings.dummy_processing = True


@pytest.fixture(scope="function", autouse=True)
def analytics_disabled():
    os.environ["NUCLIADB_DISABLE_ANALYTICS"] = "True"
    yield
    os.environ.pop("NUCLIADB_DISABLE_ANALYTICS")


@pytest.fixture(scope="function")
def tmpdir():
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    except OSError:
        # Python error on tempfile when tearing down the fixture.
        # Solved in version 3.11
        pass


@pytest.fixture(scope="function")
def endecryptor_settings():
    from nucliadb_utils.encryption.settings import settings

    secret_key = os.urandom(32)
    encoded_secret_key = base64.b64encode(secret_key).decode("utf-8")
    settings.encryption_secret_key = encoded_secret_key

    yield

    settings.encryption_secret_key = None


@pytest.fixture(scope="function")
async def nucliadb(
    endecryptor_settings,
    dummy_processing,
    analytics_disabled,
    maindb_settings,
    storage: Storage,
    storage_settings,
    tmpdir,
    learning_config,
):
    from nucliadb.common.cluster import manager

    manager.INDEX_NODES.clear()

    # we need to force DATA_PATH updates to run every test on the proper
    # temporary directory
    data_path = f"{tmpdir}/node"
    os.environ["DATA_PATH"] = data_path
    settings = Settings(
        data_path=data_path,
        http_port=free_port(),
        ingest_grpc_port=free_port(),
        train_grpc_port=free_port(),
        standalone_node_port=free_port(),
        log_format_type=LogFormatType.PLAIN,
        log_output_type=LogOutputType.FILE,
        **maindb_settings.model_dump(),
        **storage_settings,
    )

    config_nucliadb(settings)

    # Make sure tests don't write logs outside of the tmpdir
    os.environ["ERROR_LOG"] = f"{tmpdir}/logs/error.log"
    os.environ["ACCESS_LOG"] = f"{tmpdir}/logs/access.log"
    os.environ["INFO_LOG"] = f"{tmpdir}/logs/info.log"

    setup_logging(
        settings=LogSettings(
            log_output_type=LogOutputType.FILE,
            log_format_type=LogFormatType.PLAIN,
            debug=False,
            log_level=LogLevel.WARNING,
        )
    )
    server = await run_async_nucliadb(settings)
    assert server.started, "Nucliadb server did not start correctly"

    yield settings

    await maybe_cleanup_maindb()

    reset_config()
    clear_global_cache()
    await server.shutdown()


@pytest.fixture(scope="function")
async def nucliadb_reader(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://localhost:{nucliadb.http_port}/{API_PREFIX}/v1",
        timeout=None,
        event_hooks={"request": [wait_for_sync]},
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def nucliadb_writer(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "WRITER"},
        base_url=f"http://localhost:{nucliadb.http_port}/{API_PREFIX}/v1",
        timeout=None,
        event_hooks={"request": [mark_dirty]},
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def nucliadb_manager(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
        base_url=f"http://localhost:{nucliadb.http_port}/{API_PREFIX}/v1",
        timeout=None,
        event_hooks={"request": [mark_dirty]},
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def knowledgebox(nucliadb_manager: AsyncClient):
    resp = await nucliadb_manager.post("/kbs", json={"slug": "knowledgebox"})
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")

    yield uuid

    resp = await nucliadb_manager.delete(f"/kb/{uuid}")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
def pinecone_data_plane():
    dp = Mock()
    dp.upsert = AsyncMock(return_value=None)
    dp.query = AsyncMock(
        return_value=QueryResponse(
            matches=[],
        )
    )
    return dp


@pytest.fixture(scope="function")
def pinecone_control_plane():
    cp = Mock()
    cp.create_index = AsyncMock(return_value="pinecone-host")
    cp.delete_index = AsyncMock(return_value=None)
    return cp


@pytest.fixture(scope="function")
def pinecone_mock(pinecone_data_plane, pinecone_control_plane):
    pinecone_session = get_pinecone()
    pinecone_session.data_plane = Mock(return_value=pinecone_data_plane)
    pinecone_session.control_plane = Mock(return_value=pinecone_control_plane)
    yield
    clean_pinecone()


@pytest.fixture(scope="function")
async def pinecone_knowledgebox(nucliadb_manager: AsyncClient, pinecone_mock):
    resp = await nucliadb_manager.post(
        "/kbs",
        json={
            "slug": "pinecone_knowledgebox",
            "external_index_provider": {
                "type": "pinecone",
                "api_key": "my-pinecone-api-key",
                "serverless_cloud": "aws_us_east_1",
            },
        },
    )
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")

    yield uuid

    resp = await nucliadb_manager.delete(f"/kb/{uuid}")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
async def nucliadb_grpc(nucliadb: Settings):
    stub = WriterStub(aio.insecure_channel(f"localhost:{nucliadb.ingest_grpc_port}"))
    return stub


@pytest.fixture(scope="function")
async def nucliadb_train(nucliadb: Settings):
    stub = TrainStub(aio.insecure_channel(f"localhost:{nucliadb.train_grpc_port}"))
    return stub


@pytest.fixture(scope="function")
async def knowledge_graph(nucliadb_writer: AsyncClient, nucliadb_grpc: WriterStub, knowledgebox):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
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
    bm.kbid = knowledgebox
    bm.relations.extend(edges)
    await inject_message(nucliadb_grpc, bm)
    await wait_for_sync()

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/entitiesgroups",
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
        f"/kb/{knowledgebox}/entitiesgroup/scientist",
        json={"add": {}, "update": {}, "delete": ["Isaac Newsome"]},
    )
    assert resp.status_code == 200, resp.content
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/entitiesgroups",
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

    return (nodes, edges)


@pytest.fixture(scope="function")
async def stream_audit(natsd: str, mocker):
    from nucliadb_utils.audit.stream import StreamAuditStorage
    from nucliadb_utils.settings import audit_settings

    audit = StreamAuditStorage(
        [natsd],
        audit_settings.audit_jetstream_target,  # type: ignore
        audit_settings.audit_partitions,
        audit_settings.audit_hash_seed,
    )
    await audit.initialize()

    mocker.spy(audit, "send")
    mocker.spy(audit.js, "publish")
    mocker.spy(audit, "search")
    mocker.spy(audit, "chat")

    set_utility(Utility.AUDIT, audit)
    yield audit
    await audit.finalize()


@pytest.fixture(scope="function")
def predict_mock() -> Mock:  # type: ignore
    predict = get_utility(Utility.PREDICT)
    mock = Mock()
    set_utility(Utility.PREDICT, mock)

    yield mock

    if predict is None:
        clean_utility(Utility.PREDICT)
    else:
        set_utility(Utility.PREDICT, predict)


@pytest.fixture(scope="function")
def metrics_registry():
    import prometheus_client.registry

    for collector in prometheus_client.registry.REGISTRY._names_to_collectors.values():
        if not hasattr(collector, "_metrics"):
            continue
        collector._metrics.clear()
    yield prometheus_client.registry.REGISTRY


async def maybe_cleanup_maindb():
    try:
        driver = get_driver()
    except UnsetUtility:
        pass
    else:
        try:
            await cleanup_maindb(driver)
        except Exception:
            logger.error("Could not cleanup maindb on test teardown")
            pass


async def cleanup_maindb(driver: Driver):
    if not driver.initialized:
        return
    async with driver.transaction() as txn:
        all_keys = [k async for k in txn.keys("", count=-1)]
        for key in all_keys:
            await txn.delete(key)
        await txn.commit()


@pytest.fixture(scope="function")
async def txn(maindb_driver):
    async with maindb_driver.transaction() as txn:
        yield txn
        await txn.abort()


@pytest.fixture(scope="function")
async def shard_manager(storage, maindb_driver):
    mng = cluster_manager.KBShardManager()
    set_utility(Utility.SHARD_MANAGER, mng)
    yield mng
    clean_utility(Utility.SHARD_MANAGER)
