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
from typing import Any, Dict, List, Optional, Union

import nats
from nats.aio.client import Client
from nats.js.client import JetStreamContext
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_telemetry.jetstream import JetStreamContextTelemetry
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils import logger


class LocalTransactionUtility:
    async def commit(self, writer: BrokerMessage, partition: int) -> int:
        from nucliadb_utils.utilities import get_ingest

        ingest = get_ingest()

        async def iterator(writer):
            yield writer

        await ingest.ProcessMessage(iterator(writer))  # type: ignore
        return 0

    async def finalize(self):
        pass

    async def initialize(self):
        pass


class TransactionUtility:

    nc: Optional[Client] = None
    js: Optional[Union[JetStreamContext, JetStreamContextTelemetry]] = None

    def __init__(
        self,
        nats_servers: List[str],
        nats_target: str,
        nats_creds: Optional[str] = None,
        nats_index_target: Optional[str] = None,
    ):
        self.nats_creds = nats_creds
        self.nats_servers = nats_servers
        self.nats_target = nats_target
        self.nats_index_target = nats_index_target

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        logger.error("There was an error connecting to NATS transaction: {}".format(e))

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def initialize(self, service_name: Optional[str] = None):

        options: Dict[str, Any] = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
        }

        if self.nats_creds is not None:
            options["user_credentials"] = self.nats_creds

        if len(self.nats_servers) > 0:
            options["servers"] = self.nats_servers

        self.nc = await nats.connect(**options)

        jetstream = self.nc.jetstream()
        tracer_provider = get_telemetry(service_name)
        if tracer_provider is not None and jetstream is not None:
            logger.info("Configuring transaction queue with telemetry")
            self.js = JetStreamContextTelemetry(
                jetstream, f"{service_name}_transaction", tracer_provider
            )
        else:
            self.js = jetstream

    async def finalize(self):
        if self.nc:
            await self.nc.flush()
            await self.nc.close()
            self.nc = None

    async def commit(self, writer: BrokerMessage, partition: int) -> int:
        if self.js is None:
            raise AttributeError()

        res = await self.js.publish(
            self.nats_target.format(partition=partition), writer.SerializeToString()
        )
        logger.info(
            f" - Pushed message to ingest.  kb: {writer.kbid}, resource: {writer.uuid}, nucliadb seqid: {res.seq}, partition: {partition}"
        )
        return res.seq
