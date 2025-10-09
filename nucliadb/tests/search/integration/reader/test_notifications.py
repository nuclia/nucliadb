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
from unittest.mock import patch

import httpx
import pytest
import uvicorn
from httpx import AsyncClient

from nucliadb.common.nidx import NidxServiceUtility
from nucliadb.reader.api.v1.router import KB_PREFIX
from nucliadb_models.notifications import (
    Notification,
    ResourceIndexedNotification,
    ResourceProcessedNotification,
    ResourceWrittenNotification,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.utilities import MAIN, Utility
from tests.ndbfixtures.ingest import broker_resource


# `reader_api_server` depends on the dummy_nidx_utility fixture
# This fixture overrides that with actual nidx running on docker
@pytest.fixture(scope="function")
async def nidx_reader_api_server(
    reader_api_server,
    nidx,
):
    nidx_util = NidxServiceUtility("nucliadb.tests")
    await nidx_util.initialize()

    with patch.dict(MAIN, values={Utility.NIDX: nidx_util}, clear=False):
        yield reader_api_server

    await nidx_util.finalize()


@pytest.fixture(scope="function")
async def nucliadb_stream_reader(nidx_reader_api_server):
    # Our normal nucliadb_reader uses ASGITransport which does not support streaming requests
    # Instead, this runs a full unicorn server and connects via socket to emulate the full thing
    config = uvicorn.Config(nidx_reader_api_server, port=0, log_level="info")
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    while not server.started:
        await asyncio.sleep(0.1)

    port = server.servers[0].sockets[0].getsockname()[1]
    yield AsyncClient(
        base_url=f"http://localhost:{port}/api/v1",
        headers={"X-NUCLIADB-ROLES": "READER"},
    )

    task.cancel()


# This is actually a test of the reader component
# However, it sits here with search components for practical reasons:
# it requires the nidx image which is only compiled for search tests on CI
@pytest.mark.deploy_modes("component")
async def test_notification_stream(nucliadb_stream_reader, knowledgebox, processor):
    async def delayed_create_resource():
        await asyncio.sleep(0.1)

        message1 = broker_resource(knowledgebox, rid="68b6e3b747864293b71925b7bacaee7c")
        await processor.process(message1, seqid=1)

        # Some time to allow nidx to process
        await asyncio.sleep(1)

        message1.source = BrokerMessage.MessageSource.PROCESSOR
        await processor.process(message1, seqid=2)

        return "68b6e3b747864293b71925b7bacaee7c"

    # Create a resource on a delay so we get the chance to subscribe to the notifications stream
    create_resource = asyncio.create_task(delayed_create_resource())

    notifs = []
    try:
        async with nucliadb_stream_reader.stream(
            method="GET",
            url=f"/{KB_PREFIX}/{knowledgebox}/notifications",
            timeout=5,
        ) as resp:
            assert resp.status_code == 200

            async for line in resp.aiter_lines():
                notification_type = Notification.model_validate_json(line).type
                assert notification_type in [
                    "resource_indexed",
                    "resource_written",
                    "resource_processed",
                ]

                if notification_type == "resource_indexed":
                    notif = ResourceIndexedNotification.model_validate_json(line)  # type: ignore

                elif notification_type == "resource_written":
                    notif = ResourceWrittenNotification.model_validate_json(line)

                elif notification_type == "resource_processed":
                    notif = ResourceProcessedNotification.model_validate_json(line)  # type: ignore

                else:
                    assert False, "Unexpected notification type"

                notifs.append(notif)

                # Quick exit for faster testing
                if len(notifs) == 4:
                    break

    except httpx.ReadTimeout:
        pass

    resource_id = await create_resource

    assert len(notifs) == 4

    # Resource created
    assert isinstance(notifs[0], ResourceWrittenNotification)
    assert notifs[0].type == "resource_written"
    assert notifs[0].data.resource_uuid == resource_id
    assert notifs[0].data.resource_title == "Title Resource"
    assert notifs[0].data.seqid == 1

    # Resource indexed (from writer)
    assert isinstance(notifs[1], ResourceIndexedNotification)
    assert notifs[1].type == "resource_indexed"
    assert notifs[1].data.resource_uuid == resource_id
    assert notifs[1].data.resource_title == "Title Resource"
    assert notifs[1].data.seqid == 1

    # Resource processed
    assert isinstance(notifs[2], ResourceProcessedNotification)
    assert notifs[2].type == "resource_processed"
    assert notifs[2].data.resource_uuid == resource_id
    assert notifs[2].data.resource_title == "Title Resource"
    assert notifs[2].data.ingestion_succeeded is True
    assert notifs[2].data.processing_errors is False
    assert notifs[2].data.seqid == 2

    # Resource indexed (from processor)
    assert isinstance(notifs[3], ResourceIndexedNotification)
    assert notifs[3].type == "resource_indexed"
    assert notifs[3].data.resource_uuid == resource_id
    assert notifs[3].data.resource_title == "Title Resource"
    assert notifs[3].data.seqid == 2
