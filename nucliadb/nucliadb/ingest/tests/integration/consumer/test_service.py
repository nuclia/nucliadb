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
import uuid

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.transaction import TransactionUtility

pytestmark = pytest.mark.asyncio


def create_broker_message(kbid: str) -> BrokerMessage:
    bm = BrokerMessage()
    bm.uuid = uuid.uuid4().hex
    bm.kbid = kbid
    bm.texts["text1"].body = "My text1"
    bm.basic.title = "My Title"

    return bm


async def test_separated_ingest_consumer(
    ingest_consumers,
    ingest_processed_consumer,
    knowledgebox_ingest,
    transaction_utility: TransactionUtility,
    nats_manager: NatsConnectionManager,
):
    bm_normal = create_broker_message(knowledgebox_ingest)
    bm_processed = create_broker_message(knowledgebox_ingest)
    bm_processed.source == BrokerMessage.MessageSource.PROCESSOR

    await transaction_utility.commit(bm_normal, partition=1, wait=True)

    consumer_info1 = await nats_manager.js.consumer_info(
        const.Streams.INGEST.name, const.Streams.INGEST.group.format(partition="1")
    )
    consumer_info2 = await nats_manager.js.consumer_info(
        const.Streams.INGEST_PROCESSED.name, const.Streams.INGEST_PROCESSED.group
    )

    assert consumer_info1.delivered.stream_seq == 1
    assert consumer_info2.delivered.stream_seq == 0

    await transaction_utility.commit(
        bm_normal,
        partition=1,
        wait=True,
        target_subject=const.Streams.INGEST_PROCESSED.subject,
    )

    consumer_info1 = await nats_manager.js.consumer_info(
        const.Streams.INGEST.name, const.Streams.INGEST.group.format(partition="1")
    )
    consumer_info2 = await nats_manager.js.consumer_info(
        const.Streams.INGEST_PROCESSED.name, const.Streams.INGEST_PROCESSED.group
    )

    assert consumer_info1.delivered.stream_seq == 1
    assert consumer_info2.delivered.stream_seq == 2
