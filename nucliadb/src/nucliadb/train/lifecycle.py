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

from nucliadb.common.context.fastapi import inject_app_context
from nucliadb.common.nidx import start_nidx_utility, stop_nidx_utility
from nucliadb.train import SERVICE_NAME
from nucliadb.train.utils import (
    start_shard_manager,
    start_train_grpc,
    stop_shard_manager,
    stop_train_grpc,
)
from nucliadb_telemetry.utils import clean_telemetry, setup_telemetry
from nucliadb_utils.utilities import start_audit_utility, stop_audit_utility


@asynccontextmanager
async def lifespan(app: FastAPI):
    await setup_telemetry(SERVICE_NAME)
    await start_nidx_utility()
    await start_shard_manager()
    await start_train_grpc(SERVICE_NAME)
    await start_audit_utility(SERVICE_NAME)

    async with inject_app_context(app):
        yield

    await stop_audit_utility()
    await stop_train_grpc()
    await stop_shard_manager()
    await stop_nidx_utility()
    await clean_telemetry(SERVICE_NAME)
