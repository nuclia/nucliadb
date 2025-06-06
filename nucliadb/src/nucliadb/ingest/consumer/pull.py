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
import base64
import time
from contextlib import contextmanager
from typing import Optional

from aiohttp.client_exceptions import ClientConnectorError
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import extract
from opentelemetry.trace import (
    Link,
)

from nucliadb.common.http_clients.processing import (
    ProcessingHTTPClient,
    ProcessingPullMessageProgressUpdater,
    get_nua_api_id,
)
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest import SERVICE_NAME, logger, logger_activity
from nucliadb.ingest.consumer.consumer import consumer_observer
from nucliadb.ingest.orm.exceptions import ReallyStopPulling
from nucliadb.ingest.orm.processor import Processor
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_telemetry import errors
from nucliadb_telemetry.metrics import Gauge
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.settings import nuclia_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.transaction import MaxTransactionSizeExceededError
from nucliadb_utils.utilities import pull_subscriber_utilization

processing_pending_messages = Gauge("nucliadb_processing_pending_messages")


@contextmanager
def run_in_span(headers: dict[str, str]):
    # Create a span for handling this message
    tracer_provider = get_telemetry(SERVICE_NAME)
    if tracer_provider is None:
        yield
        return

    tracer = tracer_provider.get_tracer(__name__)
    our_span = tracer.start_span("handle_processing_pull")

    # Try to retrieve processing context to link to it
    witness = Context()
    processor_context = extract(headers, context=witness)
    if processor_context != witness:
        # We successfully extracted a context, we link from the processor span to ours for ease of navigation
        with tracer.start_as_current_span(
            f"Pulled from proxy", links=[Link(our_span.get_span_context())], context=processor_context
        ):
            # And link from our span back to the processor span
            our_span.add_link(trace.get_current_span().get_span_context())

    # Go back to our context
    trace.set_span_in_context(our_span)
    with trace.use_span(our_span, end_on_exit=True):
        yield


class PullV2Worker:
    """
    The pull worker is responsible for pulling messages from the pull processing
    http endpoint and processing them

    The processing pull endpoint is also described as the "processing proxy" at times.
    """

    def __init__(
        self,
        driver: Driver,
        storage: Storage,
        pull_time_error_backoff: int,
        pubsub: Optional[PubSubDriver] = None,
        pull_time_empty_backoff: float = 5.0,
        pull_api_timeout: int = 60,
    ):
        self.pull_time_error_backoff = pull_time_error_backoff
        self.pull_time_empty_backoff = pull_time_empty_backoff
        self.pull_api_timeout = pull_api_timeout

        self.processor = Processor(driver, storage, pubsub, "-1")

    async def handle_message(self, seq: int, payload: bytes) -> None:
        pb = BrokerMessage()
        data = base64.b64decode(payload)
        pb.ParseFromString(data)

        logger.debug(f"Resource: {pb.uuid} KB: {pb.kbid} ProcessingID: {pb.processing_id}")

        source = "writer" if pb.source == pb.MessageSource.WRITER else "processor"
        with consumer_observer({"source": source, "partition": "-1"}):
            await self.processor.process(
                pb,
                seq,
                transaction_check=False,
            )

    async def loop(self):
        """
        Run this forever
        """
        while True:
            try:
                await self._loop()
            except ReallyStopPulling:
                logger.info("Exiting...")
                break
            except Exception as e:
                errors.capture_exception(e)
                logger.exception("Exception on worker", exc_info=e)
                await asyncio.sleep(10)

    async def _loop(self):
        usage_metric = pull_subscriber_utilization
        headers = {}
        data = None
        if nuclia_settings.nuclia_service_account is not None:
            headers["X-STF-NUAKEY"] = f"Bearer {nuclia_settings.nuclia_service_account}"
            # parse jwt sub to get pull type id
            try:
                get_nua_api_id()
            except Exception as exc:
                logger.exception("Could not read NUA API Key. Can not start pull worker")
                raise ReallyStopPulling() from exc

        ack_tokens = []
        async with ProcessingHTTPClient() as processing_http_client:
            while True:
                try:
                    start_time = time.monotonic()

                    # The code is only really prepared to pull 1 message at a time. If changing this, review MessageProgressUpdate usage
                    pull = await processing_http_client.pull_v2(ack_tokens=ack_tokens, limit=1)
                    ack_tokens.clear()
                    if pull is None:
                        processing_pending_messages.set(0)
                        logger_activity.debug(f"No messages waiting in processing pull")
                        await asyncio.sleep(self.pull_time_empty_backoff)
                        usage_metric.inc({"status": "waiting"}, time.monotonic() - start_time)
                        continue

                    received_time = time.monotonic()
                    usage_metric.inc({"status": "waiting"}, received_time - start_time)
                    processing_pending_messages.set(pull.pending)

                    logger.info("Message received from proxy", extra={"seq": [pull.messages[0].seq]})
                    try:
                        for message in pull.messages:
                            async with ProcessingPullMessageProgressUpdater(
                                processing_http_client, message, pull.ttl * 0.66
                            ):
                                with run_in_span(message.headers):
                                    await self.handle_message(message.seq, message.payload)
                                    ack_tokens.append(message.ack_token)

                        usage_metric.inc({"status": "processing"}, time.monotonic() - received_time)
                    except Exception as e:
                        errors.capture_exception(e)
                        logger.exception("Error while pulling and processing message/s")
                        raise e

                except (
                    asyncio.exceptions.CancelledError,
                    RuntimeError,
                    KeyboardInterrupt,
                    SystemExit,
                ):
                    if ack_tokens:
                        await processing_http_client.pull_v2(ack_tokens=ack_tokens, limit=0)
                    logger.info(f"Pull task was canceled, exiting")
                    raise ReallyStopPulling()

                except ClientConnectorError:
                    logger.error(
                        f"Could not connect to processing engine, \
                         {processing_http_client.base_url} verify your internet connection"
                    )
                    await asyncio.sleep(self.pull_time_error_backoff)

                except MaxTransactionSizeExceededError as e:
                    if data is not None:
                        payload_length = 0
                        if data.payload:
                            payload_length = len(base64.b64decode(data.payload))
                        logger.error(f"Message too big for transaction: {payload_length}")
                    raise e
                except Exception:
                    logger.exception("Unhandled error pulling messages from processing")
                    await asyncio.sleep(self.pull_time_error_backoff)
