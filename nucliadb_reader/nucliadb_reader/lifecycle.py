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

from grpc import aio  # type: ignore
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb_reader import logger
from nucliadb_utils.settings import nucliadb_settings, running_settings
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_utility,
    set_utility,
    start_audit_utility,
    stop_audit_utility,
)


async def initialize() -> None:
    set_utility(
        Utility.CHANNEL, aio.insecure_channel(nucliadb_settings.nucliadb_ingest)
    )
    set_utility(Utility.INGEST, WriterStub(get_utility(Utility.CHANNEL)))

    await start_audit_utility()
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s.%(msecs)02d] [%(levelname)s] - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logger.setLevel(logging.getLevelName(running_settings.log_level.upper()))


async def finalize() -> None:
    if get_utility(Utility.CHANNEL):
        await get_utility(Utility.CHANNEL).close()
        clean_utility(Utility.CHANNEL)
        clean_utility(Utility.INGEST)
    await stop_audit_utility()
