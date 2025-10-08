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
from typing import AsyncIterator, Optional
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from starlette.responses import Response
from uvicorn.config import Config  # type: ignore
from uvicorn.server import Server  # type: ignore

from nucliadb.common.back_pressure.materializer import BackPressureMaterializer
from nucliadb.common.back_pressure.settings import BackPressureSettings
from nucliadb.common.http_clients.processing import (
    InProgressRequest,
    PulledMessage,
    PullRequestV2,
    PullResponseV2,
)
from nucliadb.ingest.consumer.pull import PullV2Worker
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.fastapi.run import start_server
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.tests import free_port


def create_broker_message(kbid: str) -> BrokerMessage:
    bm = BrokerMessage()
    bm.uuid = uuid.uuid4().hex
    bm.kbid = kbid
    bm.texts["text1"].body = "My text1"
    bm.basic.title = "My Title"
    bm.source = BrokerMessage.MessageSource.PROCESSOR

    return bm


@dataclass
class PullProcessorAPI:
    url: str
    messages: list[BrokerMessage]
    acked: list[str]


@pytest.fixture()
async def pull_processor_api():
    app = FastAPI()
    messages = []
    acked = []

    @app.get("/api/v1/internal/processing/pull")
    async def pull():
        if len(messages) == 0:
            return {"status": "empty"}
        message = messages.pop()
        return {
            "status": "ok",
            "payload": base64.b64encode(message.SerializeToString()).decode(),
            "msgid": str(len(messages)),
        }

    @app.post("/api/v2/internal/processing/pull")
    async def pull_v2(req: PullRequestV2):
        print(req)
        acked.extend(req.ack)
        if len(messages) == 0:
            return Response(status_code=204)

        message = messages.pop()
        return PullResponseV2(
            messages=[
                PulledMessage(
                    headers={},
                    ack_token="1234",
                    payload=base64.b64encode(message.SerializeToString()),
                    seq=1,
                )
            ],
            pending=len(messages),
            ttl=10,
        )

    @app.post("/api/v2/internal/processing/pull/in_progress")
    async def pull_in_progress(req: InProgressRequest):
        return Response(status_code=204)

    port = free_port()
    config = Config(app, host="0.0.0.0", port=port, http="auto")
    server = Server(config=config)

    await start_server(server, config)

    url = f"http://127.0.0.1:{port}"
    with patch(
        "nucliadb.common.http_clients.processing.nuclia_settings.nuclia_processing_cluster_url",
        url,
    ):
        yield PullProcessorAPI(url=url, messages=messages, acked=acked)

    await server.shutdown()


@pytest.fixture()
async def pull_v2_worker(maindb_driver, pull_processor_api: PullProcessorAPI, storage):
    worker = PullV2Worker(
        driver=maindb_driver,
        storage=storage,
        pull_time_error_backoff=5,
        pull_time_empty_backoff=0.1,
    )
    worker.processor = AsyncMock()

    task = asyncio.create_task(worker.loop())
    yield worker
    task.cancel()


@pytest.fixture(scope="function")
async def back_pressure(
    nats_manager: NatsConnectionManager,
) -> AsyncIterator[Optional[BackPressureMaterializer]]:
    settings = BackPressureSettings()
    settings.enabled = False
    back_pressure = BackPressureMaterializer(
        nats_manager,
        indexing_check_interval=settings.indexing_check_interval,
        ingest_check_interval=settings.ingest_check_interval,
    )
    await back_pressure.start()
    yield back_pressure
    await back_pressure.stop()


async def wait_for_messages(messages: list[BrokerMessage], max_time: int = 10) -> None:
    start = time.monotonic()
    while time.monotonic() - start < max_time:
        if len(messages) == 0:
            await asyncio.sleep(0.1)  # extra sleep to make sure it's flushed to consumer
            return

        await asyncio.sleep(0.1)


async def test_pull_v2(
    shard_manager,
    dummy_nidx_utility,
    pull_v2_worker: PullV2Worker,
    pull_processor_api: PullProcessorAPI,
    knowledgebox: str,
    nats_manager: NatsConnectionManager,
):
    # add message that should go to first consumer
    bm = create_broker_message(knowledgebox)
    pull_processor_api.messages.append(bm)

    for i in range(50):
        if len(pull_processor_api.acked) > 0:
            break

        await asyncio.sleep(0.1)

    assert pull_processor_api.messages == []
    assert pull_processor_api.acked == ["1234"]
    pull_v2_worker.processor.process.assert_awaited_with(bm, 1, transaction_check=False)  # type: ignore
