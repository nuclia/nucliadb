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

from grpc import aio
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.ingest.chitchat import ChitchatNucliaDB  # type: ignore
from nucliadb.ingest.maindb.driver import Driver
from nucliadb.ingest.settings import settings
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.utils import get_telemetry, init_telemetry
from nucliadb_utils.settings import nucliadb_settings
from nucliadb_utils.store import MAIN
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

try:
    from nucliadb.ingest.maindb.redis import RedisDriver

    REDIS = True
except ImportError:
    REDIS = False

try:
    from nucliadb.ingest.maindb.tikv import TiKVDriver

    TIKV = True
except ImportError:
    TIKV = False


try:
    from nucliadb.ingest.maindb.local import LocalDriver

    FILES = True
except ImportError:
    FILES = False


async def get_driver() -> Driver:
    if (
        settings.driver == "redis"
        and REDIS
        and "driver" not in MAIN
        and settings.driver_redis_url is not None
    ):
        redis_driver = RedisDriver(settings.driver_redis_url)
        MAIN["driver"] = redis_driver
    elif (
        settings.driver == "tikv"
        and TIKV
        and "driver" not in MAIN
        and settings.driver_tikv_url is not None
    ):
        tikv_driver = TiKVDriver(settings.driver_tikv_url)
        MAIN["driver"] = tikv_driver
    elif (
        settings.driver == "local"
        and FILES
        and "driver" not in MAIN
        and settings.driver_local_url is not None
    ):
        local_driver = LocalDriver(settings.driver_local_url)
        MAIN["driver"] = local_driver
    driver: Optional[Driver] = MAIN.get("driver")
    if driver is not None and not driver.initialized:
        await driver.initialize()
    elif driver is None:
        raise AttributeError()
    return driver


async def start_ingest(service_name: Optional[str] = None):
    actual_service = get_utility(Utility.INGEST)
    if actual_service is not None:
        return

    if nucliadb_settings.nucliadb_ingest is not None:
        # Its distributed lets create a GRPC client
        # We want Jaeger telemetry enabled
        provider = get_telemetry(service_name)

        if provider is not None:
            await init_telemetry(provider)
            otgrpc = OpenTelemetryGRPC(f"{service_name}_ingest", provider)
            channel = otgrpc.init_client(nucliadb_settings.nucliadb_ingest)
        else:
            channel = aio.insecure_channel(nucliadb_settings.nucliadb_ingest)
        set_utility(Utility.CHANNEL, channel)
        set_utility(Utility.INGEST, WriterStub(channel))
    else:
        # Its not distributed create a ingest
        from nucliadb.ingest.service.writer import WriterServicer

        service = WriterServicer()
        await service.initialize()
        set_utility(Utility.INGEST, service)


async def stop_ingest():
    if get_utility(Utility.CHANNEL):
        await get_utility(Utility.CHANNEL).close()
        clean_utility(Utility.CHANNEL)
        clean_utility(Utility.INGEST)
    if get_utility(Utility.INGEST):
        util = get_utility(Utility.INGEST)
        await util.finalize()


def get_chitchat() -> ChitchatNucliaDB:
    return get_utility(Utility.CHITCHAT)
