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
#
from contextlib import asynccontextmanager

from fastapi import FastAPI

from nucliadb.common.back_pressure import start_materializer, stop_materializer
from nucliadb.common.back_pressure.settings import settings as back_pressure_settings
from nucliadb.common.context.fastapi import inject_app_context
from nucliadb.ingest.processing import start_processing_engine, stop_processing_engine
from nucliadb.ingest.utils import start_ingest, stop_ingest
from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.tus import finalize as storage_finalize
from nucliadb.writer.tus import initialize as storage_initialize
from nucliadb_telemetry.utils import clean_telemetry, setup_telemetry
from nucliadb_utils.settings import is_onprem_nucliadb
from nucliadb_utils.utilities import (
    finalize_utilities,
    start_partitioning_utility,
    start_transaction_utility,
    stop_transaction_utility,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    back_pressure_enabled = back_pressure_settings.enabled and not is_onprem_nucliadb()

    await setup_telemetry(SERVICE_NAME)
    await start_ingest(SERVICE_NAME)
    await start_processing_engine()
    start_partitioning_utility()
    await start_transaction_utility(SERVICE_NAME)
    await storage_initialize()

    # Inject application context into the fastapi app's state
    async with inject_app_context(app) as context:
        if back_pressure_enabled:
            await start_materializer(context)
        yield

    if back_pressure_enabled:
        await stop_materializer()
    await stop_transaction_utility()
    await stop_ingest()
    await stop_processing_engine()
    await storage_finalize()
    await clean_telemetry(SERVICE_NAME)
    await finalize_utilities()
