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


@pytest.fixture()
def nats_conn():
    conn = mock.AsyncMock()
    with mock.patch("nucliadb_utils.cache.nats.Client", return_value=conn):
        yield conn


@pytest.fixture()
async def pubsub(nats_conn):
    ps = NatsPubsub()
    ps.nc = nats_conn
    yield ps


@pytest.mark.asyncio
async def test_unsubscribe_twice_raises_key_error(pubsub: NatsPubsub):
    key = "foobar"
    await pubsub.subscribe(lambda x: None, key, group="", subscription_id=key)

    await pubsub.unsubscribe(key, subscription_id=key)

    # Unsubscribing twice with the same request_id should raise KeyError
    with pytest.raises(KeyError):
        await pubsub.unsubscribe(key, subscription_id=key)
