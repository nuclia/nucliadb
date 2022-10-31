from unittest import mock

import pytest

from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.transaction import TransactionUtility, WaitFor


@pytest.mark.asyncio
async def test_transaction_commit():
    txn = TransactionUtility(
        nats_servers=["foobar"],
        nats_target="node.{node}",
        notify_subject="notify.{kbid}"
    )
    ps = NatsPubsub()
    ps.nc = mock.AsyncMock()
    txn.pubsub = ps

    waiting_for = WaitFor(uuid="foo", seq="bar")
    request_id = "request1"
    kbid = "kbid"

    _ = await txn.wait_for_commited(kbid, waiting_for, request_id=request_id)

    await txn.stop_waiting(kbid, request_id=request_id)

    # Unsubscribing twice with the same request_id should raise KeyError
    with pytest.raises(KeyError):
        await txn.stop_waiting(kbid, request_id=request_id)

