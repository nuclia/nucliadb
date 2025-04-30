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

from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.context.fastapi import inject_app_context
from nucliadb.common.maindb.utils import setup_driver
from nucliadb.common.nidx import start_nidx_utility
from nucliadb.ingest.utils import start_ingest, stop_ingest
from nucliadb.search import SERVICE_NAME
from nucliadb.search.predict import start_predict_engine
from nucliadb_telemetry.utils import clean_telemetry, setup_telemetry
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    finalize_utilities,
    get_utility,
    start_audit_utility,
    stop_audit_utility,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await setup_telemetry(SERVICE_NAME)

    await start_ingest(SERVICE_NAME)
    await start_predict_engine()

    await setup_driver()
    await setup_cluster()
    await start_nidx_utility(SERVICE_NAME)

    await start_audit_utility(SERVICE_NAME)

    async with inject_app_context(app):
        yield

    await stop_ingest()
    if get_utility(Utility.PARTITION):
        clean_utility(Utility.PARTITION)
    if get_utility(Utility.PREDICT):
        clean_utility(Utility.PREDICT)

    await finalize_utilities()
    await stop_audit_utility()
    await teardown_cluster()
    await clean_telemetry(SERVICE_NAME)
