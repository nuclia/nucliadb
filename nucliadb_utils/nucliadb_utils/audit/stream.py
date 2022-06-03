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
import asyncio
from datetime import datetime
from typing import List, Optional

import mmh3  # type: ignore
import nats
from nucliadb_protos.audit_pb2 import AuditRequest
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_utils import logger
from nucliadb_utils.audit.audit import AuditStorage


class StreamAuditStorage(AuditStorage):
    task: Optional[asyncio.Task] = None
    initialized: bool = False
    queue: asyncio.Queue
    lock: asyncio.Lock

    def __init__(
        self,
        nats_servers: List[str],
        nats_target: str,
        partitions: int,
        seed: int,
        nats_creds: Optional[str] = None,
    ):
        self.nats_servers = nats_servers
        self.nats_creds = nats_creds
        self.nats_target = nats_target
        self.partitions = partitions
        self.seed = seed
        self.lock = asyncio.Lock()
        self.queue = asyncio.Queue()

    def get_partition(self, kbid: str):
        return mmh3.hash(kbid, self.seed, signed=False) % self.partitions

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        logger.error("There was an error connecting to NATS audit: {}".format(e))

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def initialize(self):

        options = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
        }

        if self.nats_creds is not None:
            options["user_credentials"] = self.nats_creds

        if len(self.nats_servers) > 0:
            options["servers"] = self.nats_servers

        self.nc = await nats.connect(**options)

        self.js = self.nc.jetstream()
        self.task = asyncio.create_task(self.run())
        self.initialized = True

    async def finalize(self):
        if self.task is not None:
            self.task.cancel()
        if self.nc:
            await self.nc.flush()
            await self.nc.close()
            self.nc = None

    async def run(self):
        while True:
            audit = await self.queue.get()
            try:
                await self._send(audit)
            except Exception:
                logger.exception("Could not send audit", stack_info=True)

    async def send(self, message: AuditRequest):
        self.queue.put_nowait(message)

    async def _send(self, message: AuditRequest):
        if self.js is None:
            raise AttributeError()

        partition = self.get_partition(message.kbid)

        res = await self.js.publish(
            self.nats_target.format(partition=partition, type=message.type),
            message.SerializeToString(),
        )
        logger.debug(
            f"Pushed message to audit.  kb: {message.kbid}, resource: {message.rid}, partition: {partition}"
        )
        return res.seq

    async def report(self, message: BrokerMessage, audit_type: AuditRequest.AuditType.Value):  # type: ignore
        # Reports MODIFIED / DELETED / NEW events
        auditrequest = AuditRequest()
        auditrequest.kbid = message.kbid
        auditrequest.userid = message.audit.user
        auditrequest.rid = message.uuid
        auditrequest.origin = message.audit.origin
        auditrequest.type = audit_type
        auditrequest.time.CopyFrom(message.audit.when)

        for field in message.field_metadata:
            auditrequest.field_metadata.append(field.field)

        await self.send(auditrequest)

    async def visited(self, kbid: str, uuid: str, user: str, origin: str):

        auditrequest = AuditRequest()
        auditrequest.origin = origin
        auditrequest.userid = user
        auditrequest.rid = uuid
        auditrequest.kbid = kbid
        auditrequest.type = AuditRequest.VISITED
        auditrequest.time.FromDatetime(datetime.now())

        await self.send(auditrequest)

    async def search(
        self,
        kbid: str,
        user: str,
        origin: str,
        search: SearchRequest,
        timeit: float,
        resources: int,
    ):
        # Search is a base64 encoded search
        auditrequest = AuditRequest()
        auditrequest.origin = origin
        auditrequest.userid = user
        auditrequest.kbid = kbid
        auditrequest.search.CopyFrom(search)
        auditrequest.timeit = timeit
        auditrequest.resources = resources
        auditrequest.type = AuditRequest.SEARCH
        auditrequest.time.FromDatetime(datetime.now())

        await self.send(auditrequest)
