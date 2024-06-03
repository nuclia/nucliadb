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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb.common import datamanagers
from nucliadb.ingest.consumer import materializer
from nucliadb.ingest.tests.fixtures import create_resource
from nucliadb_protos import writer_pb2
from nucliadb_utils import const
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.nuclia_usage.protos.kb_usage_pb2 import KbUsage, Service
from nucliadb_utils.utilities import Utility, clean_utility, set_utility

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def nats():
    mock = AsyncMock()
    mock.jetstream = MagicMock(return_value=AsyncMock())
    yield mock


@pytest.fixture()
async def audit_storage(nats):
    with patch("nucliadb_utils.audit.stream.nats.connect", return_value=nats):
        aud = StreamAuditStorage(
            nats_servers=["nats://localhost:4222"],
            nats_target="test",
            partitions=1,
            seed=1,
            nats_creds="nats_creds",
        )
        await aud.initialize()
        set_utility(Utility.AUDIT, aud)
        yield aud
        clean_utility(Utility.AUDIT)
        await aud.finalize()


async def test_materialize_kb_data(
    maindb_driver,
    pubsub,
    storage,
    fake_node,
    knowledgebox_ingest,
    audit_storage,
):
    count = 10
    for _ in range(count):
        await create_resource(
            storage=storage,
            driver=maindb_driver,
            knowledgebox_ingest=knowledgebox_ingest,
        )

    mz = materializer.MaterializerHandler(
        driver=maindb_driver,
        storage=storage,
        pubsub=pubsub,
        check_delay=0.05,
    )
    await mz.initialize()

    async with datamanagers.with_transaction() as txn:
        assert (
            await datamanagers.resources.get_number_of_resources(
                txn, kbid=knowledgebox_ingest
            )
            == -1
        )
        assert (
            await datamanagers.resources.calculate_number_of_resources(
                txn, kbid=knowledgebox_ingest
            )
            == count
        )

    await pubsub.publish(
        const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=knowledgebox_ingest),
        writer_pb2.Notification(
            kbid=knowledgebox_ingest,
            action=writer_pb2.Notification.Action.COMMIT,
        ).SerializeToString(),
    )

    await asyncio.sleep(0.2)

    async with datamanagers.with_transaction() as txn:
        assert (
            await datamanagers.resources.get_number_of_resources(
                txn, kbid=knowledgebox_ingest
            )
            == count
        )

    await mz.finalize()

    assert audit_storage.js.publish.call_count == 1
    assert audit_storage.js.publish.call_args[0][0] == "kb-usage.nuclia_db"
    pb = KbUsage()
    pb.ParseFromString(audit_storage.js.publish.call_args[0][1])
    assert pb.storage.resources == count
    assert pb.service == Service.NUCLIA_DB
    assert pb.kb_id == knowledgebox_ingest
