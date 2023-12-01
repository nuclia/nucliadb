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

from unittest.mock import AsyncMock, patch

import pytest
from nucliadb_protos.nodewriter_pb2 import IndexMessage

from nucliadb_node.listeners import IndexedPublisher
from nucliadb_node.signals import SuccessfulIndexingPayload, successful_indexing


class TestShardGcScheduler:
    @pytest.fixture
    @pytest.mark.asyncio
    async def indexed_publisher(self):
        ip = IndexedPublisher()
        await ip.initialize()
        yield ip
        await ip.finalize()

    @pytest.fixture(autouse=True)
    def pubsub(self):
        pubsub = AsyncMock()
        with patch(
            "nucliadb_node.listeners.indexed_publisher.get_pubsub",
            new=AsyncMock(return_value=pubsub),
        ):
            yield pubsub

    @pytest.mark.asyncio
    async def test_listener_registration_in_lifecycle_functions(self):
        with patch("nucliadb_node.listeners.indexed_publisher.signals") as signals:
            ip = IndexedPublisher()
            await ip.initialize()
            assert signals.successful_indexing.add_listener.call_count == 1

            await ip.finalize()
            assert signals.successful_indexing.remove_listener.call_count == 1

    @pytest.mark.asyncio
    async def test_successful_indexing_triggers_notification(
        self, indexed_publisher, pubsub
    ):
        pb = IndexMessage()
        pb.partition = "1"
        payload = SuccessfulIndexingPayload(seqid=1, index_message=pb)

        await successful_indexing.dispatch(payload)
        assert pubsub.publish.call_count == 1
