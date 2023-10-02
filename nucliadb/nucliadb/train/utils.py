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
from typing import Optional

from grpc import aio  # type: ignore
from grpc_health.v1 import health, health_pb2_grpc

from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.train.nodes import TrainShardManager  # type: ignore
from nucliadb.train.settings import settings
from nucliadb_protos import train_pb2_grpc
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.grpc import get_traced_grpc_server
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_storage,
    get_utility,
    set_utility,
)


async def start_train_grpc(service_name: Optional[str] = None):
    actual_service = get_utility(Utility.TRAIN)
    if actual_service is not None:
        return

    aio.init_grpc_aio()  # type: ignore

    await setup_telemetry(service_name or "train")
    server = get_traced_grpc_server(service_name or "train")

    from nucliadb.train.servicer import TrainServicer

    servicer = TrainServicer()
    await servicer.initialize()
    health_servicer = health.aio.HealthServicer()  # type: ignore
    server.add_insecure_port(f"0.0.0.0:{settings.grpc_port}")

    train_pb2_grpc.add_TrainServicer_to_server(servicer, server)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await server.start()
    set_utility(Utility.TRAIN, servicer)
    set_utility(Utility.TRAIN_SERVER, server)


async def stop_train_grpc():
    if get_utility(Utility.TRAIN_SERVER):
        server = get_utility(Utility.TRAIN_SERVER)
        await server.stop(grace=False)
        clean_utility(Utility.TRAIN_SERVER)
    if get_utility(Utility.TRAIN):
        util = get_utility(Utility.TRAIN)
        await util.finalize()
        clean_utility(Utility.TRAIN)


_TRAIN_SM_NAME = "train_shard_manager"


async def start_shard_manager():
    """
    XXX this is weird but too much to untangle right now
    """
    driver = await setup_driver()
    storage = await get_storage()
    set_utility(_TRAIN_SM_NAME, TrainShardManager(driver=driver, storage=storage))


async def stop_shard_manager():
    if get_utility(_TRAIN_SM_NAME):
        clean_utility(_TRAIN_SM_NAME)
    await teardown_driver()


def get_shard_manager() -> TrainShardManager:
    util = get_utility(_TRAIN_SM_NAME)
    if util is None:
        raise AttributeError("No Node Manager defined")
    return util
