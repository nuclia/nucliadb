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
from typing import Union

import httpx
import pytest
import uvicorn
from httpx import AsyncClient
from tests.ingest.fixtures import broker_resource
from tests.utils import inject_message

from nucliadb.reader.api.v1.router import KB_PREFIX
from nucliadb_models.notifications import (
    Notification,
    ResourceIndexedNotification,
    ResourceProcessedNotification,
    ResourceWrittenNotification,
)
from nucliadb_protos import writer_pb2
from nucliadb_protos.writer_pb2 import BrokerMessage


@pytest.fixture(scope="function")
async def nucliadb_stream_reader(reader_api_server):
    # Our normal nucliadb_reader uses ASGITransport which does not support streaming requests
    # Instead, this runs a full unicorn server and connects via socket to emulate the full thing
    config = uvicorn.Config(reader_api_server, port=0, log_level="info")
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


@pytest.mark.deploy_modes("component")
async def test_activity(
    nucliadb_writer: AsyncClient, nucliadb_ingest_grpc, nucliadb_stream_reader, knowledgebox: str, pubsub
):
    async def delayed_create_resource():
        await asyncio.sleep(0.1)
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{knowledgebox}/resources",
            json={"slug": "resource1", "title": "Resource 1", "texts": {"text": {"body": "Hello!"}}},
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]

        bm = broker_resource(knowledgebox, rid)
        bm.source = BrokerMessage.MessageSource.PROCESSOR
        await inject_message(nucliadb_ingest_grpc, bm)

        # Fake indexed notification (we are using dummy nidx in this test)
        indexed = writer_pb2.Notification(
            kbid=knowledgebox, uuid=rid, seqid=123, action=writer_pb2.Notification.Action.INDEXED
        )
        await pubsub.publish(f"notify.{knowledgebox}", indexed.SerializeToString())

        return rid

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

                notif: Union[
                    ResourceIndexedNotification,
                    ResourceWrittenNotification,
                    ResourceProcessedNotification,
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
                if len(notifs) == 3:
                    break

    except httpx.ReadTimeout:
        pass

    resource_id = await create_resource

    assert len(notifs) == 3

    # Resource created
    assert isinstance(notifs[0], ResourceWrittenNotification)
    assert notifs[0].type == "resource_written"
    assert notifs[0].data.resource_uuid == resource_id
    assert notifs[0].data.resource_title == "Resource 1"
    assert notifs[0].data.operation == "created"
    assert notifs[0].data.error is False

    # Resource processed
    assert isinstance(notifs[1], ResourceProcessedNotification)
    assert notifs[1].type == "resource_processed"
    assert notifs[1].data.resource_uuid == resource_id
    assert notifs[1].data.resource_title == "Resource 1"
    assert notifs[1].data.ingestion_succeeded is True
    assert notifs[1].data.processing_errors is False

    # We get a notification for resource indexed, but it's coming from dummy nidx
    # Hard to do integration testing because currently we don't have a reader+nidx deploy mode
    # (and it's very messy to do) and notifications don't work in standalone
    assert isinstance(notifs[2], ResourceIndexedNotification)
    assert notifs[2].type == "resource_indexed"
    assert notifs[2].data.resource_uuid == resource_id
    assert notifs[2].data.resource_title == "Resource 1"
