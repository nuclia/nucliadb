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
from nucliadb_protos.writer_pb2 import BrokerMessage, Notification

from nucliadb.reader.reader.notifications import (
    kb_notifications,
    kb_notifications_stream,
)
from nucliadb_protos import writer_pb2
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.cache.settings import settings as cache_settings
from nucliadb_utils.utilities import get_pubsub

MODULE = "nucliadb.reader.reader.notifications"


@pytest.fixture(scope="function")
async def pubsub(natsd):
    cache_settings.cache_pubsub_nats_url = natsd
    pubsub = await get_pubsub()

    yield pubsub

    await pubsub.finalize()


async def test_kb_notifications(pubsub):
    kbid = "testkb"
    activity = []

    async def read_activity(kbid):
        try:
            async for chunk in kb_notifications(kbid):
                activity.append(chunk)
        except asyncio.CancelledError:
            pass

    read_task = asyncio.create_task(read_activity(kbid))
    # Let the reader task start so that the subscription is created
    await asyncio.sleep(0.1)

    # Publish some notifications
    await resource_notification(pubsub, kbid, uuid="resource1", seqid=1)
    await resource_notification(pubsub, kbid, uuid="resource2", seqid=2)
    # This notification should not be read as it is for a different kbid
    await resource_notification(pubsub, "other-kb")

    # Wait for the reader task to process the notifications
    await asyncio.sleep(1)

    # Cancel the reader task
    read_task.cancel()
    await read_task

    # Check that the activity was read
    assert len(activity) == 2
    assert activity[0].uuid == "resource1"
    assert activity[0].seqid == 1
    assert activity[1].uuid == "resource2"
    assert activity[1].seqid == 2


@pytest.fixture(scope="function")
def shorter_timeout():
    with mock.patch(f"{MODULE}.NOTIFICATIONS_TIMEOUT_S", 0.5):
        yield


async def test_kb_notifications_stream_is_cancelled(shorter_timeout, pubsub):
    stream_lines = []

    async def read_activity_stream(kbid):
        async for line in kb_notifications_stream(kbid):
            stream_lines.append(line)

    # Start a task that reads the activity stream
    read_task = asyncio.create_task(read_activity_stream("kbid"))

    # Wait for the timeout to expire and cancel the task
    await asyncio.sleep(1)

    # Check that the task was finished gracefully
    assert read_task.done()


async def resource_notification(pubsub: PubSubDriver, kbid: str, **notification_kwargs):
    """
    Publishes a 'fake' resource notification for the given kbid for testing purposes.
    """
    key = const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=kbid)
    default_notification_kwargs = {
        "partition": 1,
        "seqid": 1,
        "multi": "",
        "uuid": "123",
        "kbid": kbid,
        "action": writer_pb2.Notification.Action.COMMIT,
        "write_type": writer_pb2.Notification.WriteType.CREATED,
        "message": BrokerMessage(),
    }
    default_notification_kwargs.update(notification_kwargs)
    data = Notification(**default_notification_kwargs)  # type: ignore

    await pubsub.publish(key, data.SerializeToString())
