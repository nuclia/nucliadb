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
import time
import uuid
from dataclasses import dataclass
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from nucliadb_protos.writer_pb2 import BrokerMessage
from uvicorn.config import Config  # type: ignore
from uvicorn.server import Server  # type: ignore

from nucliadb.ingest.consumer.pull import PullWorker
from nucliadb_utils import const
from nucliadb_utils.fastapi.run import start_server
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.tests import free_port

pytestmark = pytest.mark.asyncio


def create_broker_message(kbid: str) -> BrokerMessage:
    bm = BrokerMessage()
    bm.uuid = uuid.uuid4().hex
    bm.kbid = kbid
    bm.texts["text1"].body = "My text1"
    bm.basic.title = "My Title"
    bm.source == BrokerMessage.MessageSource.PROCESSOR

    return bm


@dataclass
class PullProcessorAPI:
    url: str
    messages: list[BrokerMessage]


@pytest.fixture()
async def pull_processor_api():
    app = FastAPI()
    messages: list[BrokerMessage] = []  # type: ignore

    @app.get("/api/v1/internal/processing/pull")
    async def pull():
        if len(messages) == 0:
            return {"status": "empty"}
        message = messages.pop()
        return {
            "status": "ok",
            "payload": base64.b64encode(message.SerializeToString()).decode(),
            "msgid": len(messages),
        }

    port = free_port()
    config = Config(app, host="0.0.0.0", port=port, http="auto")
    server = Server(config=config)

    await start_server(server, config)

    url = f"http://127.0.0.1:{port}"
    with patch(
        "nucliadb.common.http_clients.processing.nuclia_settings.nuclia_processing_cluster_url",
        url,
    ):
        yield PullProcessorAPI(url=url, messages=messages)

    await server.shutdown()


@pytest.fixture()
async def pull_worker(maindb_driver, pull_processor_api: PullProcessorAPI):
    worker = PullWorker(
        driver=maindb_driver,
        partition="1",
        storage=None,  # type: ignore
        pull_time_error_backoff=5,
        pull_time_empty_backoff=0.1,
    )

    task = asyncio.create_task(worker.loop())
    yield worker
    task.cancel()


async def wait_for_messages(messages: list[BrokerMessage], max_time: int = 10) -> None:
    start = time.monotonic()
    while time.monotonic() - start < max_time:
        if len(messages) == 0:
            await asyncio.sleep(
                0.1
            )  # extra sleep to make sure it's flushed to consumer
            return

        await asyncio.sleep(0.1)


async def test_pull_full_integration(
    ingest_consumers,
    ingest_processed_consumer,
    pull_worker: PullWorker,
    pull_processor_api: PullProcessorAPI,
    knowledgebox_ingest: str,
    nats_manager: NatsConnectionManager,
):
    # make sure stream is empty
    consumer_info1 = await nats_manager.js.consumer_info(
        const.Streams.INGEST.name, const.Streams.INGEST.group.format(partition="1")
    )
    consumer_info2 = await nats_manager.js.consumer_info(
        const.Streams.INGEST_PROCESSED.name, const.Streams.INGEST_PROCESSED.group
    )
    assert consumer_info1.delivered.stream_seq == 0
    assert consumer_info2.delivered.stream_seq == 0

    # add message that should go to first consumer
    pull_processor_api.messages.append(create_broker_message(knowledgebox_ingest))
    await wait_for_messages(pull_processor_api.messages)

    consumer_info1 = await nats_manager.js.consumer_info(
        const.Streams.INGEST.name, const.Streams.INGEST_PROCESSED.group
    )

    assert consumer_info1.delivered.stream_seq == 1
