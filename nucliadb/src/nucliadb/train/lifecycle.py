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

from nucliadb.common.context import ApplicationContext
from nucliadb.common.context.fastapi import inject_app_context
from nucliadb.train import SERVICE_NAME
from nucliadb.train.utils import (
    start_shard_manager as start_train_shard_manager,
)
from nucliadb.train.utils import (
    start_train_grpc,
    stop_train_grpc,
)
from nucliadb.train.utils import (
    stop_shard_manager as stop_train_shard_manager,
)
from nucliadb_telemetry.utils import clean_telemetry, setup_telemetry


@asynccontextmanager
async def lifespan(app: FastAPI):
    await setup_telemetry(SERVICE_NAME)
    await start_train_shard_manager()
    await start_train_grpc(SERVICE_NAME)
    try:
        context = ApplicationContext(
            service_name="train",
            partitioning=False,
            nats_manager=False,
            transaction=False,
        )
        async with inject_app_context(app, context):
            yield
    finally:
        await stop_train_grpc()
        await stop_train_shard_manager()
        await clean_telemetry(SERVICE_NAME)
