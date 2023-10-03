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
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage, BrokerMessageBlobReference

from nucliadb.ingest.consumer.consumer import IngestConsumer


@pytest.fixture()
def storage():
    mock = MagicMock()
    mock.get_stream_message = AsyncMock()
    mock.del_stream_message = AsyncMock()
    yield mock


@pytest.fixture()
def consumer(storage):
    yield IngestConsumer(None, "partition", storage, None)


@pytest.mark.asyncio
async def test_get_broker_message(consumer: IngestConsumer, storage):
    bm = BrokerMessage(kbid="kbid")
    msg = Mock(data=bm.SerializeToString(), headers={})
    assert bm == await consumer.get_broker_message(msg)
    storage.get_stream_message.assert_not_called()


@pytest.mark.asyncio
async def test_get_broker_message_proxied(consumer: IngestConsumer, storage):
    bm = BrokerMessage(kbid="kbid")
    bmr = BrokerMessageBlobReference(kbid="kbid", storage_key="storage_key")
    msg = Mock(data=bmr.SerializeToString(), headers={"X-MESSAGE-TYPE": "PROXY"})

    storage.get_stream_message.return_value = bm.SerializeToString()

    assert bm == await consumer.get_broker_message(msg)

    storage.get_stream_message.assert_awaited_once_with("storage_key")


@pytest.mark.asyncio
async def test_clean_broker_message_proxied(consumer: IngestConsumer, storage):
    bmr = BrokerMessageBlobReference(kbid="kbid", storage_key="storage_key")
    msg = Mock(data=bmr.SerializeToString(), headers={"X-MESSAGE-TYPE": "PROXY"})

    await consumer.clean_broker_message(msg)

    storage.del_stream_message.assert_awaited_once_with("storage_key")
