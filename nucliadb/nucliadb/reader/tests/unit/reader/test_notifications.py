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
from unittest import mock

import pytest

from nucliadb.reader.reader.notifications import (
    kb_notifications_stream,
    serialize_notification,
)
from nucliadb_models.notifications import (
    ResourceIndexed,
    ResourceOperationType,
    ResourceProcessed,
    ResourceWritten,
)
from nucliadb_protos import writer_pb2

MODULE = "nucliadb.reader.reader.notifications"


@pytest.fixture(scope="function", autouse=True)
def timeout():
    with mock.patch(f"{MODULE}.NOTIFICATIONS_TIMEOUT_S", 1):
        yield


async def test_kb_notifications_stream_timeout_gracefully():
    event = asyncio.Event()
    cancelled_event = asyncio.Event()

    async def mocked_kb_notifications(kbid):
        # Wait longer than the timeout to yield a notification
        try:
            await asyncio.sleep(2)
            yield writer_pb2.Notification()
            event.set()
        except asyncio.CancelledError:
            cancelled_event.set()

    with mock.patch(f"{MODULE}.kb_notifications", new=mocked_kb_notifications):
        # Check that the generator returns gracefully after NOTIFICATIONS_TIMEOUT_S seconds
        async for _ in kb_notifications_stream("testkb"):
            assert False, "Should not be reached"

        assert not event.is_set()
        assert cancelled_event.is_set()


async def test_kb_notifications_stream_timeout_gracefully_while_streaming():
    cancelled_event = asyncio.Event()

    async def mocked_kb_notifications(kbid):
        # Only ever yield one notification
        try:
            yield writer_pb2.Notification()
            while True:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            cancelled_event.set()

    with mock.patch(f"{MODULE}.kb_notifications", new=mocked_kb_notifications):
        # Yield a notification first
        stream = kb_notifications_stream("testkb")
        assert await stream.__anext__()

        # Since there are no more notifications, the generator will eventually finish due to the timeout
        with pytest.raises(StopAsyncIteration):
            assert await stream.__anext__()

        # Check that the kb_notifications generator was cancelled
        assert cancelled_event.is_set()


@pytest.mark.parametrize(
    "pb,serialized_data",
    [
        (
            writer_pb2.Notification(
                uuid="rid",
                seqid=1,
                source=writer_pb2.NotificationSource.PROCESSOR,
                processing_errors=True,
            ),
            ResourceProcessed(
                resource_uuid="rid",
                seqid=1,
                ingestion_succeeded=True,
                processing_errors=True,
            ),
        ),
        (
            writer_pb2.Notification(
                uuid="rid",
                seqid=1,
                source=writer_pb2.NotificationSource.WRITER,
                write_type=writer_pb2.Notification.WriteType.DELETED,
                action=writer_pb2.Notification.Action.ABORT,
            ),
            ResourceWritten(
                resource_uuid="rid",
                seqid=1,
                operation=ResourceOperationType.DELETED,
                error=True,
            ),
        ),
        (
            writer_pb2.Notification(
                uuid="rid",
                seqid=1,
                action=writer_pb2.Notification.Action.INDEXED,
            ),
            ResourceIndexed(
                resource_uuid="rid",
                seqid=1,
            ),
        ),
    ],
)
def test_serialize_notification(pb, serialized_data):
    serialized = serialize_notification(pb)
    assert serialized.data == serialized_data
