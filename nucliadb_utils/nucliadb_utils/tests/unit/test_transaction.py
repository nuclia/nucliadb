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

from unittest import mock

import pytest
from nucliadb_protos.writer_pb2 import Notification

from nucliadb_utils.transaction import TransactionUtility, WaitFor


@pytest.fixture()
def pubsub():
    pubsub = mock.AsyncMock()
    pubsub.parse = lambda msg: msg.data
    with mock.patch("nucliadb_utils.transaction.get_pubsub", return_value=pubsub):
        yield pubsub


@pytest.fixture()
async def txn(pubsub):
    nats_conn = mock.AsyncMock()
    with mock.patch("nucliadb_utils.transaction.nats.connect", return_value=nats_conn):
        txn = TransactionUtility(nats_servers=["foobar"], nats_target="node.{node}")
        await txn.initialize()
        yield txn
        await txn.finalize()


@pytest.mark.asyncio
async def test_wait_for_commited(txn: TransactionUtility, pubsub):
    waiting_for = WaitFor(uuid="foo")
    request_id = "request1"
    kbid = "kbid"
    notification = Notification()
    notification.uuid = waiting_for.uuid
    notification.action = Notification.Action.COMMIT

    async def _subscribe(handler, key, subscription_id):
        # call it immediately
        handler(mock.Mock(data=notification.SerializeToString()))

    pubsub.subscribe.side_effect = _subscribe

    await (await txn.wait_for_commited(kbid, waiting_for, request_id=request_id)).wait()


@pytest.mark.asyncio
async def test_wait_for_indexed(txn: TransactionUtility, pubsub):
    waiting_for = WaitFor(uuid="foo")
    request_id = "request1"
    kbid = "kbid"
    notification = Notification()
    notification.uuid = waiting_for.uuid
    notification.action = Notification.Action.INDEXED

    async def _subscribe(handler, key, subscription_id):
        # call it immediately
        handler(mock.Mock(data=notification.SerializeToString()))

    pubsub.subscribe.side_effect = _subscribe

    with mock.patch("nucliadb_utils.transaction.has_feature", return_value=True):
        await (
            await txn.wait_for_commited(kbid, waiting_for, request_id=request_id)
        ).wait()


@pytest.mark.asyncio
async def test_wait_for_commit_stop_waiting(txn: TransactionUtility, pubsub):
    pubsub.unsubscribe.side_effect = [
        "sub_id",
        KeyError(),
    ]  # second call raises KeyError

    waiting_for = WaitFor(uuid="foo", seq=1)
    request_id = "request1"
    kbid = "kbid"

    _ = await txn.wait_for_commited(kbid, waiting_for, request_id=request_id)

    await txn.stop_waiting(kbid, request_id=request_id)

    # Unsubscribing twice with the same request_id should raise KeyError
    with pytest.raises(KeyError):
        await txn.stop_waiting(kbid, request_id=request_id)
