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

from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

import pytest

from nucliadb_utils.signals import Signal


class TestSignals:
    @dataclass
    class TestPayload:
        payload: int

    @pytest.fixture
    def signal(self):
        yield Signal(payload_model=TestSignals.TestPayload)

    @pytest.fixture
    def payload(self):
        yield TestSignals.TestPayload(payload=1)

    @pytest.fixture
    def listener(self):
        yield AsyncMock()

    # Tests

    def test_add_same_listener_twice_fails(self, signal, listener):
        id = "test-listener"
        signal.add_listener(id, listener)
        with pytest.raises(ValueError):
            signal.add_listener(id, listener)

    async def test_signal_dispatch(self, signal, listener, payload):
        id = "test-listener"
        signal.add_listener(id, listener)
        await signal.dispatch(payload)
        assert listener.await_count == 1

    async def test_signal_despatch_with_failure(self, signal, listener, payload):
        ok_listener = AsyncMock()
        signal.add_listener("ok-listener", ok_listener)
        nok_listener = AsyncMock(side_effect=Exception)
        signal.add_listener("nok-listener", nok_listener)

        with patch("nucliadb_utils.signals.capture_exception") as mock:
            await signal.dispatch(payload)
            assert mock.call_count == 1

        assert ok_listener.await_count == 1
        assert nok_listener.await_count == 1

    async def test_remove_listener(self, signal, listener, payload):
        id = "test-listener"

        signal.add_listener(id, listener)
        await signal.dispatch(payload)
        assert listener.await_count == 1

        signal.remove_listener(id)
        await signal.dispatch(payload)
        assert listener.await_count == 1
