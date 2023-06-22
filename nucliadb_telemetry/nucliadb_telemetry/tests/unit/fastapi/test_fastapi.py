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
from unittest.mock import Mock

from nucliadb_telemetry.fastapi import instrument_app
from nucliadb_telemetry.fastapi.tracing import CaptureTraceIdMiddleware


def test_instrument_app_adds_capture_trace_id_middleware():
    app = Mock()
    instrument_app(app, [])
    for middleware_call in app.add_middleware.call_args_list:
        assert middleware_call[0][0] != CaptureTraceIdMiddleware

    app = Mock()
    instrument_app(app, [], trace_id_on_responses=True)
    assert app.add_middleware.call_args_list[0][0][0] == CaptureTraceIdMiddleware
