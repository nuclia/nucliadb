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
import logging
import sys

from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat

from nucliadb.train import SERVICE_NAME, logger
from nucliadb.train.chitchat import start_chitchat, stop_chitchat
from nucliadb.train.utils import (
    start_nodes_manager,
    start_train_grpc,
    stop_nodes_manager,
    stop_train_grpc,
)
from nucliadb_telemetry.utils import clean_telemetry, get_telemetry, init_telemetry
from nucliadb_utils.settings import running_settings
from nucliadb_utils.utilities import start_audit_utility, stop_audit_utility


async def initialize() -> None:
    tracer_provider = get_telemetry(SERVICE_NAME)
    if tracer_provider:
        set_global_textmap(B3MultiFormat())
        await init_telemetry(tracer_provider)

    await start_chitchat(SERVICE_NAME)
    await start_nodes_manager()
    await start_train_grpc(SERVICE_NAME)
    await start_audit_utility()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s.%(msecs)02d] [%(levelname)s] - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logger.setLevel(logging.getLevelName(running_settings.log_level.upper()))


async def finalize() -> None:
    await stop_audit_utility()
    await stop_train_grpc()
    await stop_nodes_manager()
    await stop_chitchat()
    await clean_telemetry(SERVICE_NAME)
