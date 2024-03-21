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
    get_resource_title,
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
    context = mock.Mock()
    event = asyncio.Event()
    cancelled_event = asyncio.Event()

    async def mocked_kb_notifications(kbid):
        # Wait longer than the timeout to yield a notification
        try:
            await asyncio.sleep(2)
            yield writer_pb2.Notification(source=writer_pb2.NotificationSource.WRITER)
            event.set()
        except asyncio.CancelledError:
            cancelled_event.set()

    with mock.patch(f"{MODULE}.kb_notifications", new=mocked_kb_notifications):
        # Check that the generator returns gracefully after NOTIFICATIONS_TIMEOUT_S seconds
        async for _ in kb_notifications_stream(context, "testkb"):
            assert False, "Should not be reached"

        assert not event.is_set()
        assert cancelled_event.is_set()


@pytest.fixture(scope="function")
def get_resource_title_mock():
    with mock.patch(
        "nucliadb.reader.reader.notifications.get_resource_title",
        return_value="Resource",
    ) as m:
        yield m


async def test_kb_notifications_stream_timeout_gracefully_while_streaming(
    get_resource_title_mock,
):
    context = mock.Mock()
    cancelled_event = asyncio.Event()

    async def mocked_kb_notifications(kbid):
        # Only ever yield one notification
        try:
            yield writer_pb2.Notification(source=writer_pb2.NotificationSource.WRITER)
            while True:
                await asyncio.sleep(0.1)
        except asyncio.CancelledError:
            cancelled_event.set()

    with mock.patch(f"{MODULE}.kb_notifications", new=mocked_kb_notifications):
        # Yield a notification first
        stream = kb_notifications_stream(context, "testkb")
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
                resource_title="Resource",
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
                resource_title="Resource",
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
                resource_title="Resource",
                resource_uuid="rid",
                seqid=1,
            ),
        ),
    ],
)
async def test_serialize_notification(pb, serialized_data, get_resource_title_mock):
    context = mock.Mock()
    cache = {}
    serialized = await serialize_notification(context, pb, cache)
    assert serialized.data == serialized_data


async def test_serialize_notification_caches_resource_titles(get_resource_title_mock):
    cache = {}
    notif = writer_pb2.Notification(
        uuid="rid",
        seqid=1,
        action=writer_pb2.Notification.Action.INDEXED,
    )
    await serialize_notification(mock.Mock(), notif, cache)
    assert cache == {"rid": "Resource"}
    get_resource_title_mock.assert_called_once()

    # Check that the cache is used
    await serialize_notification(mock.Mock(), notif, cache)
    get_resource_title_mock.assert_called_once()


@pytest.fixture(scope="function")
def get_resource_basic():
    with mock.patch(
        "nucliadb.reader.reader.notifications.datamanagers.resources.get_resource_basic"
    ) as m:
        yield m


@pytest.fixture(scope="function")
def kv_driver():
    txn = mock.Mock()
    driver = mock.MagicMock()
    driver.transaction.return_value.__aenter__.return_value = txn
    return driver


async def test_get_resource_title(kv_driver, get_resource_basic):
    basic = mock.Mock(title="Resource")
    get_resource_basic.return_value = basic

    assert await get_resource_title(kv_driver, "kbid", "rid") == "Resource"

    get_resource_basic.return_value = None
    assert await get_resource_title(kv_driver, "kbid", "rid") is None
