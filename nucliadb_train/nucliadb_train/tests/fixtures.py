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
import uuid
from datetime import datetime

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_ingest.orm.resource import KB_RESOURCE_SLUG_BASE
from nucliadb_utils.utilities import Utility, set_utility
from grpc import aio


def free_port() -> int:
    import socket

    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


@pytest.fixture(scope="function")
def test_settings_train(cache, gcs, fake_node, redis_driver):  # type: ignore
    from nucliadb_utils.settings import running_settings, storage_settings
    from nucliadb_train.settings import settings

    running_settings.debug = False
    print(f"Redis ready at {redis_driver.url}")

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = "gcs"
    storage_settings.gcs_bucket = "test"
    settings.grpc_port = free_port()

    set_utility(Utility.CACHE, cache)
    yield


@pytest.fixture(scope="function")
async def train_api(test_settings_train: None, local_files, event_loop):  # type: ignore
    from nucliadb_train.server import start_grpc

    finalizer = await start_grpc("testing_train")
    yield

    finalizer()


@pytest.fixture(scope="function")
async def train_client(train_api):  # type: ignore
    from nucliadb_protos.train_pb2_grpc import TrainStub
    from nucliadb_train.settings import settings

    channel = aio.insecure_channel(f"localhost:{settings.grpc_port}")
    yield TrainStub(channel)


def broker_simple_resource(knowledgebox, number):
    rid = str(uuid.uuid4())
    message1: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=str(number),
        type=BrokerMessage.AUTOCOMMIT,
    )

    message1.basic.icon = "text/plain"
    message1.basic.title = str(number)
    message1.basic.summary = "Summary of document"
    message1.basic.thumbnail = "doc"
    message1.basic.layout = "default"
    message1.basic.metadata.useful = True
    message1.basic.metadata.language = "es"
    message1.basic.created.FromDatetime(datetime.utcnow())
    message1.basic.modified.FromDatetime(datetime.utcnow())
    message1.source = BrokerMessage.MessageSource.WRITER
    FieldComputedMetadataWrapper()
    message1.field_metadata.append()

    return message1


@pytest.fixture(scope="function")
async def test_pagination_resources(processor, knowledgebox, test_settings_train):
    """
    Create a set of resources with only basic information to test pagination
    """

    amount = 10
    for i in range(1, 10 + 1):
        message = broker_simple_resource(knowledgebox, i)
        await processor.process(message=message, seqid=i)
        # Give processed data some time to reach the node

    from time import time

    from nucliadb_ingest.utils import get_driver

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

    yield knowledgebox
