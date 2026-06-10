# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
from unittest import mock

import nats
import nats.errors
import pytest

from nucliadb_protos.writer_pb2 import BrokerMessage, Notification
from nucliadb_utils.transaction import (
    MaxTransactionSizeExceededError,
    TransactionCommitTimeoutError,
    TransactionUtility,
    WaitFor,
)


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
        txn = TransactionUtility(nats_servers=["foobar"])
        await txn.initialize()
        yield txn
        await txn.finalize()


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

    waiter = await txn.wait_for_commited(kbid, waiting_for, request_id=request_id)
    assert waiter
    await waiter.wait()


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


async def test_commit_timeout(txn: TransactionUtility, pubsub):
    txn.js = mock.AsyncMock()
    bm = BrokerMessage()

    waiting_event = mock.Mock(wait=mock.Mock(side_effect=asyncio.TimeoutError))
    txn.wait_for_commited = mock.AsyncMock(return_value=waiting_event)  # type: ignore

    with pytest.raises(TransactionCommitTimeoutError):
        await txn.commit(bm, 1, wait=True, target_subject="foo")


async def test_max_payload_error_handled(txn: TransactionUtility, pubsub):
    txn.js = mock.Mock()
    txn.js.publish = mock.AsyncMock(side_effect=nats.errors.MaxPayloadError)
    bm = BrokerMessage()
    with pytest.raises(MaxTransactionSizeExceededError):
        await txn.commit(bm, 1, target_subject="foo")
