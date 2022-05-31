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

from pydantic import BaseSettings


class TelemetrySettings(BaseSettings):
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 6831
    jaeger_enabled: bool = False
    jaeger_query_port: int = 16686
    jaeger_query_host: str = "jaeger.observability.svc.cluster.local"


telemetry_settings = TelemetrySettings()
