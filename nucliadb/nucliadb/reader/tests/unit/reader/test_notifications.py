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

from nucliadb.reader.reader.notifications import kb_notifications_stream
from nucliadb_protos import writer_pb2


async def test_kb_notifications_stream_timeout_gracefully():
    event = asyncio.Event()

    async def mocked_long_kb_notifications(kbid):
        await asyncio.sleep(2)
        yield writer_pb2.Notification()
        event.set()

    module = "nucliadb.reader.reader.notifications"
    with mock.patch(f"{module}.NOTIFICATIONS_TIMEOUT_S", 1), mock.patch(
        f"{module}.kb_notifications", new=mocked_long_kb_notifications
    ):
        # Check that the generator returns after NOTIFICATIONS_TIMEOUT_S seconds

        async for _ in kb_notifications_stream("testkb"):
            assert False, "Should not be reached"

        assert not event.is_set()
