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
