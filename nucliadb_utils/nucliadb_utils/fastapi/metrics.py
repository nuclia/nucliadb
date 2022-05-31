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

import prometheus_client  # type: ignore
from fastapi import FastAPI
from starlette.responses import PlainTextResponse

from nucliadb_utils.settings_running import running_settings


async def metrics(request):
    output = prometheus_client.exposition.generate_latest()
    return PlainTextResponse(output.decode("utf8"))


application_metrics = FastAPI(title="NucliaDB Metrics API", debug=running_settings.debug)  # type: ignore
application_metrics.add_route("/metrics", metrics)
