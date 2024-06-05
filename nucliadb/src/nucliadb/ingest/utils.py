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

from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.common.maindb.utils import setup_driver
from nucliadb_utils.grpc import get_traced_grpc_channel
from nucliadb_utils.settings import nucliadb_settings
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


async def start_ingest(service_name: Optional[str] = None):
    await setup_driver()

    actual_service = get_utility(Utility.INGEST)
    if actual_service is not None:
        return

    if nucliadb_settings.nucliadb_ingest is not None:
        # Its distributed lets create a GRPC client
        # We want Jaeger telemetry enabled
        channel = get_traced_grpc_channel(
            nucliadb_settings.nucliadb_ingest, service_name or "ingest"
        )
        set_utility(Utility.CHANNEL, channel)
        ingest = WriterStub(channel)  # type: ignore
        set_utility(Utility.INGEST, ingest)
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
