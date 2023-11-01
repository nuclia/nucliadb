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
import tempfile
from os.path import dirname
from typing import AsyncIterator
from unittest.mock import Mock

import asyncpg
import pytest
from grpc import aio  # type: ignore
from httpx import AsyncClient
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.utils_pb2 import Relation, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from pytest_lazy_fixtures import lazy_fixture
from redis import asyncio as aioredis

from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.local import LocalDriver
from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.redis import RedisDriver
from nucliadb.common.maindb.tikv import TiKVDriver
from nucliadb.common.maindb.utils import _DRIVER_UTIL_NAME, get_driver
from nucliadb.ingest.settings import DriverConfig, DriverSettings
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.standalone.config import config_nucliadb
from nucliadb.standalone.run import run_async_nucliadb
from nucliadb.standalone.settings import Settings
from nucliadb.tests.utils import inject_message
from nucliadb.writer import API_PREFIX
from nucliadb_utils.storages.settings import settings as storage_settings
from nucliadb_utils.store import MAIN
from nucliadb_utils.tests import free_port
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    clear_global_cache,
    get_utility,
    set_utility,
)

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def dummy_processing():
    from nucliadb_utils.settings import nuclia_settings

    nuclia_settings.dummy_processing = True


@pytest.fixture(scope="function", autouse=True)
def analytics_disabled():
    os.environ["NUCLIADB_DISABLE_ANALYTICS"] = "True"
    yield
    os.environ.pop("NUCLIADB_DISABLE_ANALYTICS")


def reset_config():
    from nucliadb.ingest import settings as ingest_settings
    from nucliadb.train import settings as train_settings
    from nucliadb.writer import settings as writer_settings
    from nucliadb_utils import settings as utils_settings
    from nucliadb_utils.cache import settings as cache_settings

    ingest_settings.settings.parse_obj(ingest_settings.Settings())
    train_settings.settings.parse_obj(train_settings.Settings())
    writer_settings.settings.parse_obj(writer_settings.Settings())
    cache_settings.settings.parse_obj(cache_settings.Settings())

    utils_settings.audit_settings.parse_obj(utils_settings.AuditSettings())
    utils_settings.indexing_settings.parse_obj(utils_settings.IndexingSettings())
    utils_settings.transaction_settings.parse_obj(utils_settings.TransactionSettings())
    utils_settings.nucliadb_settings.parse_obj(utils_settings.NucliaDBSettings())
    utils_settings.nuclia_settings.parse_obj(utils_settings.NucliaSettings())
    utils_settings.storage_settings.parse_obj(utils_settings.StorageSettings())

    yield

    from nucliadb.common.cluster import manager

    manager.INDEX_NODES.clear()


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
async def nucliadb(dummy_processing, analytics_disabled, driver_settings, tmpdir):
    from nucliadb.common.cluster import manager

    manager.INDEX_NODES.clear()

    # we need to force DATA_PATH updates to run every test on the proper
    # temporary directory
    data_path = f"{tmpdir}/node"
    os.environ["DATA_PATH"] = data_path
    settings = Settings(
        file_backend="local",
        local_files=f"{tmpdir}/blob",
        data_path=data_path,
        http_port=free_port(),
        ingest_grpc_port=free_port(),
        train_grpc_port=free_port(),
        standalone_node_port=free_port(),
        **driver_settings.dict(),
    )

    config_nucliadb(settings)
    server = await run_async_nucliadb(settings)

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
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def nucliadb_writer(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "WRITER"},
        base_url=f"http://localhost:{nucliadb.http_port}/{API_PREFIX}/v1",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def nucliadb_manager(nucliadb: Settings):
    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
        base_url=f"http://localhost:{nucliadb.http_port}/{API_PREFIX}/v1",
    ) as client:
        yield client


@pytest.fixture(scope="function")
async def knowledgebox(nucliadb_manager: AsyncClient, request):
    resp = await nucliadb_manager.post(
        "/kbs", json={"slug": "knowledgebox", "release_channel": request.param}
    )
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")

    yield uuid

    resp = await nucliadb_manager.delete(f"/kb/{uuid}")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
async def nucliadb_grpc(nucliadb: Settings):
    stub = WriterStub(aio.insecure_channel(f"localhost:{nucliadb.ingest_grpc_port}"))  # type: ignore
    return stub


@pytest.fixture(scope="function")
async def nucliadb_train(nucliadb: Settings):
    stub = TrainStub(aio.insecure_channel(f"localhost:{nucliadb.train_grpc_port}"))  # type: ignore
    return stub


@pytest.fixture(scope="function")
async def knowledge_graph(
    nucliadb_writer: AsyncClient, nucliadb_grpc: WriterStub, knowledgebox
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"X-SYNCHRONOUS": "True"},
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "summary": "Test knowledge graph",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    nodes = {
        "Animal": RelationNode(
            value="Animal", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Batman": RelationNode(
            value="Batman", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Becquer": RelationNode(
            value="Becquer", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Cat": RelationNode(
            value="Cat", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Catwoman": RelationNode(
            value="Catwoman", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Eric": RelationNode(
            value="Eric", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Fly": RelationNode(
            value="Fly", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Gravity": RelationNode(
            value="Gravity", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Joan Antoni": RelationNode(
            value="Joan Antoni", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Joker": RelationNode(
            value="Joker", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Newton": RelationNode(
            value="Newton", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Physics": RelationNode(
            value="Physics", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Poetry": RelationNode(
            value="Poetry", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
        "Swallow": RelationNode(
            value="Swallow", ntype=RelationNode.NodeType.ENTITY, subtype=""
        ),
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

    return (nodes, edges)


@pytest.fixture(scope="function")
async def stream_audit(natsd: str):
    from nucliadb_utils.audit.stream import StreamAuditStorage
    from nucliadb_utils.settings import audit_settings

    audit = StreamAuditStorage(
        [natsd],
        audit_settings.audit_jetstream_target,  # type: ignore
        audit_settings.audit_partitions,
        audit_settings.audit_hash_seed,
    )
    await audit.initialize()
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
    import prometheus_client.registry  # type: ignore

    for collector in prometheus_client.registry.REGISTRY._names_to_collectors.values():
        if not hasattr(collector, "_metrics"):
            continue
        collector._metrics.clear()
    yield prometheus_client.registry.REGISTRY


@pytest.fixture(scope="function")
async def redis_config(redis):
    ingest_settings.driver_redis_url = f"redis://{redis[0]}:{redis[1]}"
    default_driver = ingest_settings.driver

    ingest_settings.driver = "redis"

    storage_settings.local_testing_files = f"{dirname(__file__)}"
    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    yield ingest_settings.driver_redis_url

    ingest_settings.driver_redis_url = None
    ingest_settings.driver = default_driver
    await driver.flushall()
    await driver.close(close_connection_pool=True)

    pubsub = get_utility(Utility.PUBSUB)
    if pubsub is not None:
        await pubsub.finalize()


@pytest.fixture(scope="function")
def local_driver_settings(tmpdir):
    return DriverSettings(
        driver=DriverConfig.LOCAL,
        driver_local_url=f"{tmpdir}/main",
    )


@pytest.fixture(scope="function")
async def local_driver(local_driver_settings) -> AsyncIterator[Driver]:
    path = local_driver_settings.driver_local_url
    ingest_settings.driver = DriverConfig.LOCAL
    ingest_settings.driver_local_url = path

    driver: Driver = LocalDriver(url=path)
    await driver.initialize()

    yield driver

    await driver.finalize()

    ingest_settings.driver_local_url = None
    MAIN.pop("driver", None)


@pytest.fixture(scope="function")
def tikv_driver_settings(tikvd):
    if os.environ.get("TESTING_TIKV_LOCAL", None):
        url = "localhost:2379"
    else:
        url = f"{tikvd[0]}:{tikvd[2]}"
    return DriverSettings(driver=DriverConfig.TIKV, driver_tikv_url=[url])


@pytest.fixture(scope="function")
async def tikv_driver(tikv_driver_settings) -> AsyncIterator[Driver]:
    url = tikv_driver_settings.driver_tikv_url
    ingest_settings.driver = DriverConfig.TIKV
    ingest_settings.driver_tikv_url = url

    driver: Driver = TiKVDriver(url=url)
    await driver.initialize()

    yield driver

    txn = await driver.begin()
    async for key in txn.keys(""):
        await txn.delete(key)
    await txn.commit()
    await driver.finalize()
    ingest_settings.driver_tikv_url = None
    MAIN.pop("driver", None)


@pytest.fixture(scope="function")
def redis_driver_settings(redis):
    return DriverSettings(
        driver=DriverConfig.REDIS,
        driver_redis_url=f"redis://{redis[0]}:{redis[1]}",
    )


@pytest.fixture(scope="function")
async def redis_driver(redis_driver_settings) -> AsyncIterator[RedisDriver]:
    url = redis_driver_settings.driver_redis_url
    ingest_settings.driver = DriverConfig.REDIS
    ingest_settings.driver_redis_url = url

    driver = RedisDriver(url=url)
    await driver.initialize()

    assert driver.redis is not None
    await driver.redis.flushall()
    logging.info(f"Redis driver ready at {url}")

    set_utility("driver", driver)

    yield driver

    await driver.finalize()
    ingest_settings.driver_redis_url = None
    MAIN.pop("driver", None)


@pytest.fixture(scope="function")
def pg_driver_settings(pg):
    url = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/postgres"
    return DriverSettings(
        driver=DriverConfig.PG,
        driver_pg_url=url,
    )


@pytest.fixture(scope="function")
async def pg_driver(pg_driver_settings):
    url = pg_driver_settings.driver_pg_url
    ingest_settings.driver = DriverConfig.PG
    ingest_settings.driver_pg_url = url

    conn = await asyncpg.connect(url)
    await conn.execute(
        """
DROP table IF EXISTS resources;
"""
    )
    await conn.close()
    driver = PGDriver(url=url)
    await driver.initialize()

    yield driver

    await driver.finalize()
    ingest_settings.driver_pg_url = None


def driver_settings_lazy_fixtures(default_drivers="local"):
    driver_types = os.environ.get("TESTING_MAINDB_DRIVERS", default_drivers)
    return [
        lazy_fixture.lf(f"{driver_type}_driver_settings")
        for driver_type in driver_types.split(",")
    ]


@pytest.fixture(scope="function", params=driver_settings_lazy_fixtures())
def driver_settings(request):
    """
    Allows dynamically loading the driver fixtures via env vars.

    MAINDB_DRIVER=redis,local pytest nucliadb/nucliadb/tests/

    Any test using the nucliadb fixture will be run twice, once with redis driver and once with local driver.
    """
    yield request.param


def driver_lazy_fixtures(default_drivers: str = "redis"):
    """
    Allows running tests using maindb_driver for each supported driver type via env vars.

    MAINDB_DRIVER=redis,local pytest nucliadb/nucliadb/ingest/tests/

    Any test using the maindb_driver fixture will be run twice, once with redis_driver and once with local_driver.
    """
    driver_types = os.environ.get("TESTING_MAINDB_DRIVERS", default_drivers)
    return [
        lazy_fixture.lf(f"{driver_type}_driver")
        for driver_type in driver_types.split(",")
    ]


@pytest.fixture(
    scope="function",
    params=driver_lazy_fixtures(),
)
async def maindb_driver(request):
    driver = request.param
    MAIN[_DRIVER_UTIL_NAME] = driver

    yield driver

    await cleanup_maindb(driver)
    MAIN.pop(_DRIVER_UTIL_NAME, None)


async def maybe_cleanup_maindb():
    try:
        driver = get_driver()
    except KeyError:
        pass
    else:
        try:
            await cleanup_maindb(driver)
        except Exception:
            logger.error(f"Could not cleanup maindb on test teardown")
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
    txn = await maindb_driver.begin()
    yield txn
    await txn.abort()


@pytest.fixture(scope="function")
async def shard_manager(gcs_storage, maindb_driver):
    mng = cluster_manager.KBShardManager()
    set_utility(Utility.SHARD_MANAGER, mng)
    yield mng
    clean_utility(Utility.SHARD_MANAGER)
