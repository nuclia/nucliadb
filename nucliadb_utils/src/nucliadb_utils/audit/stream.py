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
import contextvars
import time
from datetime import datetime, timezone
from typing import Callable, List, Optional

import backoff
import mmh3
import nats
from fastapi import Request
from google.protobuf.timestamp_pb2 import Timestamp
from opentelemetry.trace import format_trace_id, get_current_span
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response, StreamingResponse
from starlette.types import ASGIApp

from nucliadb_protos.audit_pb2 import (
    AuditField,
    AuditRequest,
    ChatContext,
    ClientType,
)
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.resources_pb2 import FieldID
from nucliadb_utils import logger
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.nats import get_traced_jetstream
from nucliadb_utils.nuclia_usage.protos.kb_usage_pb2 import (
    ClientType as ClientTypeKbUsage,
)
from nucliadb_utils.nuclia_usage.protos.kb_usage_pb2 import (
    KBSource,
    Search,
    SearchType,
    Service,
    Storage,
)
from nucliadb_utils.nuclia_usage.utils.kb_usage_report import KbUsageReportUtility


class RequestContext:
    def __init__(self):
        self.audit_request: AuditRequest = AuditRequest()
        self.start_time: float = time.monotonic()


request_context_var = contextvars.ContextVar[Optional[RequestContext]]("request_context", default=None)


def get_trace_id() -> str:
    span = get_current_span()
    if span is None:
        return ""
    return format_trace_id(span.get_span_context().trace_id)


def get_request_context() -> Optional[RequestContext]:
    return request_context_var.get()


class AuditMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, audit_utility_getter: Callable[[], Optional[AuditStorage]]) -> None:
        self.audit_utility_getter = audit_utility_getter
        super().__init__(app)

    @property
    def audit_utility(self):
        return self.audit_utility_getter()

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        context = RequestContext()
        token = request_context_var.set(context)
        context.audit_request.time.FromDatetime(datetime.now(tz=timezone.utc))
        context.audit_request.trace_id = get_trace_id()
        response = await call_next(request)

        if isinstance(response, StreamingResponse):
            response = self.wrap_streaming_response(response, context)
        else:
            self.enqueue_pending(context)

        request_context_var.reset(token)

        return response

    def enqueue_pending(self, context: RequestContext):
        if context.audit_request.kbid:
            # an audit request with no kbid makes no sense, we use this as an heuristic
            # mark that no audit has been set during this request

            context.audit_request.request_time = time.monotonic() - context.start_time
            if self.audit_utility is not None:
                self.audit_utility.send(context.audit_request)

    def wrap_streaming_response(
        self, response: StreamingResponse, context: RequestContext
    ) -> StreamingResponse:
        """
        When dealing with streaming responses, AND if we depend on any state that only will be available once
        the request is fully finished, the response we have after the dispatch call_next is not enough, as
        there, no iteration of the streaming response has been done yet.

        This is why we need to rewrap to be able to to the auditing at the _real_ request end without losing
        any audit bits.
        """
        original_body_iterator = response.body_iterator

        async def custom_body_iterator():
            try:
                async for chunk in original_body_iterator:
                    yield chunk
            finally:
                self.enqueue_pending(context)

        response.body_iterator = custom_body_iterator()
        return response


KB_USAGE_STREAM_SUBJECT = "kb-usage.nuclia_db"


class StreamAuditStorage(AuditStorage):
    task: Optional[asyncio.Task]
    initialized: bool
    queue: asyncio.Queue

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
        self.queue = asyncio.Queue()
        self.service = service
        self.task = None
        self.initialized = False

    def get_partition(self, kbid: str):
        return mmh3.hash(kbid, self.seed, signed=False) % self.partitions

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        logger.error("There was an error connecting to NATS audit: {}".format(e), exc_info=True)

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

        self.kb_usage_utility = KbUsageReportUtility(
            nats_stream=self.js, nats_subject=KB_USAGE_STREAM_SUBJECT
        )
        await self.kb_usage_utility.initialize()

        self.initialized = True

    async def finalize(self):
        await self.kb_usage_utility.finalize()

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

    def send(self, message: AuditRequest):
        self.queue.put_nowait(message)

    @backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=4)
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

    def report_and_send(
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
    ):
        auditrequest = AuditRequest()

        # Reports MODIFIED / DELETED / NEW events

        auditrequest.trace_id = get_trace_id()
        auditrequest.kbid = kbid
        auditrequest.userid = user or ""
        auditrequest.rid = rid or ""
        auditrequest.origin = origin or ""
        auditrequest.type = audit_type
        # If defined, when needs to overwrite any previously set time
        if not (when is None or when.SerializeToString() == b""):
            auditrequest.time.CopyFrom(when)

        auditrequest.field_metadata.extend(field_metadata or [])

        if audit_fields:
            auditrequest.fields_audit.extend(audit_fields)

        self.send(auditrequest)

    def report_fields_and_paragraphs(self, kbid: str, paragraphs: int, fields: int):
        self.kb_usage_utility.send_kb_usage(
            service=Service.NUCLIA_DB,
            account_id=None,
            kb_id=kbid,
            kb_source=KBSource.HOSTED,
            storage=Storage(paragraphs=paragraphs, fields=fields),
        )

    def report_resources(
        self,
        *,
        kbid: str,
        resources: int,
    ):
        self.kb_usage_utility.send_kb_usage(
            service=Service.NUCLIA_DB,
            account_id=None,
            kb_id=kbid,
            kb_source=KBSource.HOSTED,
            storage=Storage(resources=resources),
        )

    def visited(
        self,
        kbid: str,
        uuid: str,
        user: str,
        origin: str,
        send: bool = False,
    ):
        context = get_request_context()
        if context is None:
            return
        auditrequest = context.audit_request

        auditrequest.origin = origin
        auditrequest.userid = user
        auditrequest.rid = uuid
        auditrequest.kbid = kbid
        auditrequest.type = AuditRequest.VISITED

    def delete_kb(self, kbid: str):
        self.kb_usage_utility.send_kb_usage(
            service=Service.NUCLIA_DB,
            account_id=None,
            kb_id=kbid,
            kb_source=KBSource.HOSTED,
            storage=Storage(paragraphs=0, fields=0, resources=0),
        )

    def search(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        search: SearchRequest,
        timeit: float,
        resources: int,
    ):
        context = get_request_context()
        if context is None:
            return

        auditrequest = context.audit_request

        auditrequest.origin = origin
        auditrequest.client_type = client_type  # type: ignore
        auditrequest.userid = user
        auditrequest.kbid = kbid
        auditrequest.search.CopyFrom(search)
        auditrequest.retrieval_time = timeit
        auditrequest.resources = resources
        auditrequest.type = AuditRequest.SEARCH

        self.kb_usage_utility.send_kb_usage(
            service=Service.NUCLIA_DB,
            account_id=None,
            kb_id=kbid,
            kb_source=KBSource.HOSTED,
            # TODO unify AuditRequest client type and Nuclia Usage client type
            searches=[
                Search(
                    client=ClientTypeKbUsage.Value(ClientType.Name(client_type)),  # type: ignore
                    type=SearchType.SEARCH,
                    tokens=2000,
                    num_searches=1,
                )
            ],
        )

    def suggest(
        self,
        kbid: str,
        client_type: int,
    ):
        self.kb_usage_utility.send_kb_usage(
            service=Service.NUCLIA_DB,
            account_id=None,
            kb_id=kbid,
            kb_source=KBSource.HOSTED,
            # TODO unify AuditRequest client type and Nuclia Usage client type
            searches=[
                Search(
                    client=ClientTypeKbUsage.Value(ClientType.Name(client_type)),  # type: ignore
                    type=SearchType.SUGGEST,
                    tokens=0,
                    num_searches=1,
                )
            ],
        )

    def chat(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        question: str,
        rephrased_question: Optional[str],
        context: List[ChatContext],
        answer: Optional[str],
        learning_id: str,
        rephrase_time: Optional[float] = None,
        generative_answer_time: Optional[float] = None,
        generative_answer_first_chunk_time: Optional[float] = None,
    ):
        rcontext = get_request_context()
        if rcontext is None:
            return

        auditrequest = rcontext.audit_request

        auditrequest.origin = origin
        auditrequest.client_type = client_type  # type: ignore
        auditrequest.userid = user
        auditrequest.kbid = kbid
        if rephrase_time is not None:
            auditrequest.rephrase_time = rephrase_time
        if generative_answer_time is not None:
            auditrequest.generative_answer_time = generative_answer_time
        if generative_answer_first_chunk_time is not None:
            auditrequest.generative_answer_first_chunk_time = generative_answer_first_chunk_time
        auditrequest.type = AuditRequest.CHAT
        auditrequest.chat.question = question
        auditrequest.chat.context.extend(context)
        auditrequest.chat.learning_id = learning_id
        if rephrased_question is not None:
            auditrequest.chat.rephrased_question = rephrased_question
        if answer is not None:
            auditrequest.chat.answer = answer
