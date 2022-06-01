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

import asyncio
import math
import socket
from asyncio import Future
from functools import partial
from typing import List

from opentelemetry.exporter.jaeger.thrift import JaegerExporter  # type: ignore
from opentelemetry.exporter.jaeger.thrift.gen.agent import Agent  # type: ignore
from opentelemetry.exporter.jaeger.thrift.gen.jaeger import Collector  # type: ignore
from opentelemetry.exporter.jaeger.thrift.translate import Translate  # type: ignore
from opentelemetry.exporter.jaeger.thrift.translate import (  # type: ignore
    ThriftTranslator,
)
from opentelemetry.sdk.resources import SERVICE_NAME  # type: ignore
from opentelemetry.sdk.trace import Span  # type: ignore
from opentelemetry.sdk.trace.export import SpanExportResult  # type: ignore
from thrift.protocol import TCompactProtocol  # type: ignore
from thrift.transport import TTransport  # type: ignore

from nucliadb_telemetry import logger

UDP_PACKET_MAX_LENGTH = 65000


class JaegerExporterAsync(JaegerExporter):
    def __init__(self, **kwags):
        super(JaegerExporterAsync, self).__init__(**kwags)
        self._agent_client = AgentClientUDPAsync(
            host_name=self.agent_host_name,
            port=self.agent_port,
            split_oversized_batches=self.udp_split_oversized_batches,
        )

    async def export(self, spans: List[Span]) -> SpanExportResult:
        # Populate service_name from first span
        # We restrict any SpanProcessor to be only associated with a single
        # TracerProvider, so it is safe to assume that all Spans in a single
        # batch all originate from one TracerProvider (and in turn have all
        # the same service.name)
        if len(spans) == 0:
            return SpanExportResult.SUCCESS
        if spans:
            service_name = spans[0].resource.attributes.get(SERVICE_NAME)
            if service_name:
                self.service_name = service_name
        translator = Translate(spans)
        thrift_translator = ThriftTranslator(self._max_tag_value_length)
        jaeger_spans = translator._translate(thrift_translator)
        batch = Collector.Batch(
            spans=jaeger_spans,
            process=Collector.Process(serviceName=self.service_name),
        )
        if self._collector_http_client is not None:
            raise Exception("Not supported on asyncio")
            # self._collector_http_client.submit(batch)
        else:
            await self._agent_client.emit(batch)

        return SpanExportResult.SUCCESS


class JaegerClientProtocol:
    def __init__(self, message: bytes, on_con_lost: Future):
        self.message = message
        self.on_con_lost = on_con_lost
        self.transport = None

    def error_received(self, exc):
        logger.exception("Error received from Jaeger", exc_info=exc)
        if not self.on_con_lost.done():
            self.on_con_lost.set_result(False)

    def connection_lost(self, exc):
        logger.exception("Connection lost with Jaeger", exc_info=exc)
        if not self.on_con_lost.done():
            self.on_con_lost.set_result(True)

    def connection_made(self, transport):
        self.transport = transport
        self.transport.sendto(self.message)


class AgentClientUDPAsync:
    """Implement a UDP client to agent.

    Args:
        host_name: The host name of the Jaeger server.
        port: The port of the Jaeger server.
        max_packet_size: Maximum size of UDP packet.
        client: Class for creating new client objects for agencies.
        split_oversized_batches: Re-emit oversized batches in smaller chunks.
    """

    def __init__(
        self,
        host_name,
        port,
        max_packet_size=UDP_PACKET_MAX_LENGTH,
        client=Agent.Client,
        split_oversized_batches=False,
    ):
        self.host_name = host_name
        self.port = port
        self.max_packet_size = max_packet_size
        self.buffer = TTransport.TMemoryBuffer()
        self.client = client(iprot=TCompactProtocol.TCompactProtocol(trans=self.buffer))
        self.split_oversized_batches = split_oversized_batches
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setblocking(False)
        self._addr = (host_name, int(port))

    async def emit(self, batch: Collector.Batch):
        """
        Args:
            batch: Object to emit Jaeger spans.
        """

        # pylint: disable=protected-access
        self.client._seqid = 0
        #  truncate and reset the position of BytesIO object
        self.buffer._buffer.truncate(0)
        self.buffer._buffer.seek(0)
        self.client.emitBatch(batch)
        buff = self.buffer.getvalue()
        if len(buff) > self.max_packet_size:
            if self.split_oversized_batches and len(batch.spans) > 1:
                packets = math.ceil(len(buff) / self.max_packet_size)
                div = math.ceil(len(batch.spans) / packets)
                for packet in range(packets):
                    start = packet * div
                    end = (packet + 1) * div
                    if start < len(batch.spans):
                        await self.emit(
                            Collector.Batch(
                                process=batch.process,
                                spans=batch.spans[start:end],
                            )
                        )
            else:
                logger.warning(
                    "Data exceeds the max UDP packet size; size %r, max %r",
                    len(buff),
                    self.max_packet_size,
                )
            return

        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()

        send_to = partial(self._sendto, buff, on_con_lost)
        loop.add_writer(self._sock.fileno(), send_to)
        try:
            await on_con_lost
        except Exception:
            logger.exception("Exception on sending to jaeger", stack_info=True)
        finally:
            loop.remove_writer(self._sock.fileno())

    def _sendto(self, buff, on_con_lost: asyncio.Future):
        try:
            self._sock.sendto(buff, self._addr)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as exc:
            on_con_lost.set_exception(exc)
        except Exception as exc:
            on_con_lost.set_exception(exc)
        else:
            on_con_lost.set_result(True)
