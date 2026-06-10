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


async def test_unsubscribe_twice_raises_key_error(pubsub: NatsPubsub):
    key = "foobar"
    await pubsub.subscribe(lambda x: None, key, group="", subscription_id=key)  # type: ignore[ty:invalid-argument-type]

    await pubsub.unsubscribe(key, subscription_id=key)

    # Unsubscribing twice with the same request_id should raise KeyError
    with pytest.raises(KeyError):
        await pubsub.unsubscribe(key, subscription_id=key)
