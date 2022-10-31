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

from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.transaction import TransactionUtility, WaitFor


@pytest.mark.asyncio
async def test_transaction_commit():
    txn = TransactionUtility(
        nats_servers=["foobar"],
        nats_target="node.{node}",
        notify_subject="notify.{kbid}",
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
