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


from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.app import application
from nucliadb_telemetry.fastapi import instrument_app
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.fastapi.run import run_fastapi_with_metrics


def run():
    setup_logging()
    instrument_app(
        application,
        tracer_provider=get_telemetry(SERVICE_NAME),
        excluded_urls=["/"],
        metrics=True,
        trace_id_on_responses=True,
    )

    run_fastapi_with_metrics(application)
