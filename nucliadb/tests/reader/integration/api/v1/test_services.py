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
from collections.abc import AsyncGenerator
from datetime import datetime
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from nucliadb.common.http_clients import processing
from nucliadb.reader.api.v1.router import KB_PREFIX
from nucliadb_models.notifications import (
    Notification,
    ResourceIndexedNotification,
    ResourceProcessedNotification,
    ResourceWrittenNotification,
)
from nucliadb_protos import writer_pb2


@pytest.fixture(scope="function", autouse=True)
def get_resource_title():
    with mock.patch(
        "nucliadb.reader.reader.notifications.get_resource_title",
        return_value="Resource",
    ) as m:
        yield m


@pytest.fixture(scope="function")
def kb_notifications():
    async def _kb_notifications(
        kbid: str,
    ) -> AsyncGenerator[writer_pb2.Notification, None]:
        for notification in [
            writer_pb2.Notification(
                kbid=kbid,
                seqid=1,
                uuid="resource",
                write_type=writer_pb2.Notification.WriteType.CREATED,
                action=writer_pb2.Notification.Action.COMMIT,
                source=writer_pb2.NotificationSource.WRITER,
            ),
            writer_pb2.Notification(
                kbid=kbid,
                seqid=1,
                uuid="resource",
                write_type=writer_pb2.Notification.WriteType.CREATED,
                action=writer_pb2.Notification.Action.COMMIT,
                source=writer_pb2.NotificationSource.PROCESSOR,
                processing_errors=True,
            ),
            writer_pb2.Notification(
                kbid=kbid,
                seqid=1,
                uuid="resource",
                write_type=writer_pb2.Notification.WriteType.CREATED,
                action=writer_pb2.Notification.Action.INDEXED,
            ),
        ]:
            await asyncio.sleep(0.001)
            yield notification

    with mock.patch(
        "nucliadb.reader.reader.notifications.kb_notifications", new=_kb_notifications
    ) as mocked:
        yield mocked


@pytest.mark.deploy_modes("component")
async def test_activity(kb_notifications, nucliadb_reader: AsyncClient, knowledgebox: str):
    kbid = knowledgebox
    async with nucliadb_reader.stream(
        method="GET",
        url=f"/{KB_PREFIX}/{kbid}/notifications",
    ) as resp:
        assert resp.status_code == 200

        notifs = []
        async for line in resp.aiter_lines():
            notification_type = Notification.model_validate_json(line).type
            assert notification_type in [
                "resource_indexed",
                "resource_written",
                "resource_processed",
            ]

            notif: (
                ResourceIndexedNotification | ResourceWrittenNotification | ResourceProcessedNotification
            )

            if notification_type == "resource_indexed":
                notif = ResourceIndexedNotification.model_validate_json(line)  # type: ignore
                assert notif.type == "resource_indexed"
                assert notif.data.resource_uuid == "resource"
                assert notif.data.resource_title == "Resource"
                assert notif.data.seqid == 1

            elif notification_type == "resource_written":
                notif = ResourceWrittenNotification.model_validate_json(line)
                assert notif.type == "resource_written"
                assert notif.data.resource_uuid == "resource"
                assert notif.data.resource_title == "Resource"
                assert notif.data.seqid == 1
                assert notif.data.operation == "created"
                assert notif.data.error is False

            elif notification_type == "resource_processed":
                notif = ResourceProcessedNotification.model_validate_json(line)  # type: ignore
                assert notif.type == "resource_processed"
                assert notif.data.resource_uuid == "resource"
                assert notif.data.resource_title == "Resource"
                assert notif.data.seqid == 1
                assert notif.data.ingestion_succeeded is True
                assert notif.data.processing_errors is True

            else:
                assert False, "Unexpected notification type"

            notifs.append(notif)

    assert len(notifs) == 3


@pytest.mark.deploy_modes("component")
async def test_activity_kb_not_found(
    nucliadb_reader: AsyncClient,
):
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/foobar/notifications")
    assert resp.status_code == 404


@pytest.mark.deploy_modes("component")
async def test_processing_status(
    nucliadb_reader: AsyncClient,
    simple_resources: tuple[str, list[str]],
):
    kbid, resources = simple_resources

    processing_client = MagicMock()
    processing_client.__aenter__ = AsyncMock(return_value=processing_client)
    processing_client.__aexit__ = AsyncMock(return_value=None)
    processing_client.requests = AsyncMock(
        return_value=processing.RequestsResults(
            results=[
                processing.RequestsResult(  # type: ignore
                    processing_id="processing_id",
                    resource_id=resource_id,
                    kbid=kbid,
                    completed=False,
                    scheduled=False,
                    timestamp=datetime.now(),
                )
                for resource_id in resources
            ],
            cursor=None,
        )
    )
    with patch(
        "nucliadb.reader.api.v1.services.processing.ProcessingHTTPClient",
        return_value=processing_client,
    ):
        resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{kbid}/processing-status")
        assert resp.status_code == 200

        data = processing.RequestsResults.model_validate(resp.json())

        assert all([result.title is not None for result in data.results])
