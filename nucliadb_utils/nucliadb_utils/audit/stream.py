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

import backoff
import mmh3  # type: ignore
import nats
from google.protobuf.timestamp_pb2 import Timestamp
from nucliadb_protos.audit_pb2 import (
    AuditField,
    AuditKBCounter,
    AuditRequest,
    ChatContext,
)
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.resources_pb2 import FieldID
from opentelemetry.trace import format_trace_id, get_current_span

from nucliadb_utils import logger
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.nats import get_traced_jetstream


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
        service: str = "nucliadb.audit",
    ):
        self.nats_servers = nats_servers
        self.nats_creds = nats_creds
        self.nats_target = nats_target
        self.partitions = partitions
        self.seed = seed
        self.lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        self.service = service

    def get_partition(self, kbid: str):
        return mmh3.hash(kbid, self.seed, signed=False) % self.partitions

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        logger.error(
            "There was an error connecting to NATS audit: {}".format(e), exc_info=True
        )

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def initialize(self):
        options = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
        }

        if self.nats_creds:
            options["user_credentials"] = self.nats_creds

        if len(self.nats_servers) > 0:
            options["servers"] = self.nats_servers

        self.nc = await nats.connect(**options)

        self.js = get_traced_jetstream(self.nc, self.service)
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
            item_dequeued = False
            try:
                audit = await self.queue.get()
                item_dequeued = True
                await self._send(audit)
            except (asyncio.CancelledError, KeyboardInterrupt, RuntimeError):
                return
            except Exception:  # pragma: no cover
                logger.exception("Could not send audit", stack_info=True)
            finally:
                if item_dequeued:
                    self.queue.task_done()

    async def send(self, message: AuditRequest):
        self.queue.put_nowait(message)

    @backoff.on_exception(backoff.expo, (Exception,), max_tries=4)
    async def _send(self, message: AuditRequest):
        if self.js is None:  # pragma: no cover
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

    async def report(
        self,
        *,
        kbid: str,
        audit_type: AuditRequest.AuditType.Value,  # type: ignore
        when: Optional[Timestamp] = None,
        user: Optional[str] = None,
        origin: Optional[str] = None,
        rid: Optional[str] = None,
        field_metadata: Optional[List[FieldID]] = None,
        audit_fields: Optional[List[AuditField]] = None,
        kb_counter: Optional[AuditKBCounter] = None,
    ):
        # Reports MODIFIED / DELETED / NEW events
        auditrequest = AuditRequest()
        auditrequest.kbid = kbid
        auditrequest.userid = user or ""
        auditrequest.rid = rid or ""
        auditrequest.origin = origin or ""
        auditrequest.type = audit_type
        if when is None or when.SerializeToString() == b"":
            auditrequest.time.FromDatetime(datetime.now())
        else:
            auditrequest.time.CopyFrom(when)

        auditrequest.field_metadata.extend(field_metadata or [])

        if audit_fields:
            auditrequest.fields_audit.extend(audit_fields)

        if kb_counter:
            auditrequest.kb_counter.CopyFrom(kb_counter)

        auditrequest.trace_id = get_trace_id()

        await self.send(auditrequest)

    async def visited(self, kbid: str, uuid: str, user: str, origin: str):
        auditrequest = AuditRequest()
        auditrequest.origin = origin
        auditrequest.userid = user
        auditrequest.rid = uuid
        auditrequest.kbid = kbid
        auditrequest.type = AuditRequest.VISITED
        auditrequest.time.FromDatetime(datetime.now())

        auditrequest.trace_id = get_trace_id()

        await self.send(auditrequest)

    async def delete_kb(self, kbid):
        # Search is a base64 encoded search
        auditrequest = AuditRequest()
        auditrequest.kbid = kbid
        auditrequest.type = AuditRequest.KB_DELETED
        auditrequest.time.FromDatetime(datetime.now())
        auditrequest.trace_id = get_trace_id()
        await self.send(auditrequest)

    async def search(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        search: SearchRequest,
        timeit: float,
        resources: int,
    ):
        # Search is a base64 encoded search
        auditrequest = AuditRequest()
        auditrequest.origin = origin
        auditrequest.client_type = client_type  # type: ignore
        auditrequest.userid = user
        auditrequest.kbid = kbid
        auditrequest.search.CopyFrom(search)
        auditrequest.timeit = timeit
        auditrequest.resources = resources
        auditrequest.type = AuditRequest.SEARCH
        auditrequest.time.FromDatetime(datetime.now())

        auditrequest.trace_id = get_trace_id()
        await self.send(auditrequest)

    async def suggest(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        timeit: float,
    ):
        auditrequest = AuditRequest()
        auditrequest.origin = origin
        auditrequest.client_type = client_type  # type: ignore
        auditrequest.userid = user
        auditrequest.kbid = kbid
        auditrequest.timeit = timeit
        auditrequest.type = AuditRequest.SUGGEST
        auditrequest.time.FromDatetime(datetime.now())
        auditrequest.trace_id = get_trace_id()

        await self.send(auditrequest)

    async def chat(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        timeit: float,
        question: str,
        rephrased_question: Optional[str],
        context: List[ChatContext],
        answer: Optional[str],
    ):
        auditrequest = AuditRequest()
        auditrequest.origin = origin
        auditrequest.client_type = client_type  # type: ignore
        auditrequest.userid = user
        auditrequest.kbid = kbid
        auditrequest.timeit = timeit
        auditrequest.type = AuditRequest.CHAT
        auditrequest.time.FromDatetime(datetime.now())
        auditrequest.trace_id = get_trace_id()
        auditrequest.chat.question = question
        auditrequest.chat.context.extend(context)
        if rephrased_question is not None:
            auditrequest.chat.rephrased_question = rephrased_question
        if answer is not None:
            auditrequest.chat.answer = answer
        await self.send(auditrequest)


def get_trace_id() -> str:
    span = get_current_span()
    if span is None:
        return ""
    return format_trace_id(span.get_span_context().trace_id)
