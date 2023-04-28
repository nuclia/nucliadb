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
import logging
import time
from typing import List, Optional

import aiohttp
import nats
from aiohttp.client_exceptions import ClientConnectorError
from aiohttp.web import Response
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest import SERVICE_NAME, logger, logger_activity
from nucliadb.ingest.maindb.driver import Driver
from nucliadb.ingest.orm.exceptions import (
    DeadletteredError,
    ReallyStopPulling,
    SequenceOrderViolation,
)
from nucliadb.ingest.orm.processor import Processor
from nucliadb_telemetry import errors, metrics
from nucliadb_telemetry.utils import set_info_on_span
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.cache import KB_COUNTER_CACHE
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.exceptions import ShardsNotFound
from nucliadb_utils.nats import get_traced_jetstream
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_transaction_utility

consumer_observer = metrics.Observer(
    "message_processor",
    labels={"source": ""},
    buckets=[
        0.01,
        0.025,
        0.05,
        0.1,
        0.5,
        1.0,
        2.5,
        5.0,
        7.5,
        10.0,
        30.0,
        60.0,
        120.0,
        float("inf"),
    ],
    error_mappings={"deadlettered": DeadletteredError, "shardnotfound": ShardsNotFound},
)


class PullWorker:
    subscriptions: List[Subscription]

    def __init__(
        self,
        driver: Driver,
        partition: str,
        storage: Storage,
        pull_time: int,
        zone: str,
        nuclia_cluster_url: str,
        nuclia_public_url: str,
        audit: Optional[AuditStorage],
        target: str,
        group: str,
        stream: str,
        onprem: bool,
        cache: Optional[Cache] = None,
        service_name: Optional[str] = None,
        nats_creds: Optional[str] = None,
        nats_servers: Optional[List[str]] = None,
        creds: Optional[str] = None,
        local_subscriber: bool = False,
    ):
        self.driver = driver
        self.partition = partition
        self.storage = storage
        self.pull_time = pull_time
        self.audit = audit
        self.zone = zone
        self.nuclia_cluster_url = nuclia_cluster_url
        self.nuclia_public_url = nuclia_public_url
        self.local_subscriber = local_subscriber
        self.nats_subscriber = not local_subscriber
        self.creds = creds
        self.cache = cache
        self.nats_creds = nats_creds
        self.nats_servers = nats_servers or []
        self.target = target
        self.group = group
        self.stream = stream
        self.onprem = onprem
        self.service_name = service_name
        self.idle_heartbeat = 5 * 1_000_000_000
        self.ack_wait = 10 * 60
        self.nc = None
        self.js = None
        self.initialized = False

        self.subscriptions = []

        self.lock = asyncio.Lock()
        self.processor = Processor(driver, storage, audit, cache, partition)

    async def disconnected_cb(self):
        logger.info(
            f"PullWorker[partition={self.partition}]: Got disconnected from NATS!"
        )

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.warning(
            f"PullWorker[partition={self.partition}]: \
                Got reconnected to NATS {self.nc.connected_url.netloc}. Attempting to re-subscribe."
        )
        await self.drain_subscriptions()
        await self.setup_nats_subscription()

    async def error_cb(self, e):
        msg = f"PullWorker[partition={self.partition}]: There was an error on consumer: {e}"
        logger.error(msg, exc_info=True)

    async def closed_cb(self):
        logger.info(
            f"PullWorker[partition={self.partition}]: Connection is closed on NATS"
        )

    async def initialize(self):
        await self.processor.initialize()

        if self.nats_subscriber:
            options = {
                "error_cb": self.error_cb,
                "closed_cb": self.closed_cb,
                "reconnected_cb": self.reconnected_cb,
            }

            if self.nats_creds is not None:
                options["user_credentials"] = self.nats_creds

            if len(self.nats_servers) > 0:
                options["servers"] = self.nats_servers

            try:
                self.nc = await nats.connect(**options)
            except Exception:
                pass

            if self.nc is not None:
                self.js = get_traced_jetstream(self.nc, SERVICE_NAME)
                await self.setup_nats_subscription()

        self.initialized = True

    async def setup_nats_subscription(self):
        last_seqid = await self.processor.driver.last_seqid(self.partition)
        if last_seqid is None:
            last_seqid = 1
        self.subscriptions.append(
            await self.js.subscribe(
                subject=self.target.format(partition=self.partition),
                queue=self.group.format(partition=self.partition),
                stream=self.stream,
                flow_control=True,
                cb=self.subscription_worker,
                config=nats.js.api.ConsumerConfig(
                    deliver_policy=nats.js.api.DeliverPolicy.BY_START_SEQUENCE,
                    opt_start_seq=last_seqid,
                    ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                    max_ack_pending=1,
                    max_deliver=10000,
                    ack_wait=self.ack_wait,
                    idle_heartbeat=5.0,
                ),
            )
        )
        logger.info(
            f"Subscribed to {self.target.format(partition=self.partition)} \
                        on stream {self.stream} from {last_seqid}"
        )

    async def drain_subscriptions(self) -> None:
        for subscription in self.subscriptions:
            try:
                await subscription.drain()
            except nats.errors.ConnectionClosedError:
                pass
        self.subscriptions = []

    async def finalize(self):
        await self.drain_subscriptions()
        if self.nats_subscriber and self.nc is not None:
            try:
                await self.nc.drain()
            except nats.errors.ConnectionClosedError:
                pass
            await self.nc.close()

    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(reply.split(".")[5])
        logger.info(
            f"Message received: subject:{subject}, seqid: {seqid}, reply: {reply}"
        )
        message_source = "<msg source not set>"
        start = time.monotonic()

        async with self.lock:
            try:
                pb = BrokerMessage()
                pb.ParseFromString(msg.data)
                if pb.source == pb.MessageSource.PROCESSOR:
                    message_source = "processing"
                elif pb.source == pb.MessageSource.WRITER:
                    message_source = "writer"
                if pb.HasField("audit"):
                    audit_time = pb.audit.when.ToDatetime().isoformat()
                else:
                    audit_time = ""

                logger.debug(
                    f"Received from {message_source} on {pb.kbid}/{pb.uuid} seq {seqid} partition {self.partition} at {time}"  # noqa
                )
                set_info_on_span({"nuclia.kbid": pb.kbid, "nuclia.rid": pb.uuid})

                try:
                    with consumer_observer(
                        {
                            "source": "writer"
                            if pb.source == pb.MessageSource.WRITER
                            else "processor"
                        }
                    ):
                        await self.processor.process(pb, seqid, self.partition)
                except SequenceOrderViolation as err:
                    log_func = logger.error
                    if seqid == err.last_seqid:  # pragma: no cover
                        # Occasional retries of the last processed message may happen
                        log_func = logger.warning
                    log_func(
                        f"Old txn: DISCARD (nucliadb seqid: {seqid}, partition: {self.partition}). Current seqid: {err.last_seqid}"  # noqa
                    )
                else:
                    message_type_name = pb.MessageType.Name(pb.type)
                    time_to_process = time.monotonic() - start
                    log_level = (
                        logging.INFO if time_to_process < 10 else logging.WARNING
                    )
                    logger.log(
                        log_level,
                        f"Successfully processed {message_type_name} message from \
                            {message_source}. kb: {pb.kbid}, resource: {pb.uuid}, \
                                nucliadb seqid: {seqid}, partition: {self.partition} as {audit_time}, \
                                    total time: {time_to_process:.2f}s",
                    )
                    if self.cache is not None:
                        await self.cache.delete(
                            KB_COUNTER_CACHE.format(kbid=pb.kbid), invalidate=True
                        )
            except DeadletteredError as e:
                # Messages that have been sent to deadletter at some point
                # We don't want to process it again so it's ack'd
                errors.capture_exception(e)
                logger.info(
                    f"An error happend while processing a message from {message_source}. "
                    f"A copy of the message has been stored on {self.processor.storage.deadletter_bucket}. "
                    f"Check sentry for more details: {str(e)}"
                )
                await msg.ack()
            except (ShardsNotFound,) as e:
                # Any messages that for some unexpected inconsistency have failed and won't be tried again
                # as we cannot do anything about it
                # - ShardsNotFound: /kb/{id}/shards key or the whole /kb/{kbid} is missing
                errors.capture_exception(e)
                logger.info(
                    f"An error happend while processing a message from {message_source}. "
                    f"This message has been dropped and won't be retried again"
                    f"Check sentry for more details: {str(e)}"
                )
                await msg.ack()
            except Exception as e:
                # Unhandled exceptions that need to be retried after a small delay
                errors.capture_exception(e)
                logger.info(
                    f"An error happend while processing a message from {message_source}. "
                    "Message has not been ACKd and will be retried. "
                    f"Check sentry for more details: {str(e)}"
                )
                await asyncio.sleep(2)
                raise e
            else:
                # Successful processing
                await msg.ack()

    async def loop(self):
        while self.initialized is False:
            try:
                await self.initialize()
            except Exception as e:
                errors.capture_exception(e)
                logger.exception("Exception on initializing worker", exc_info=e)
                await asyncio.sleep(10)

        if self.pull_time == 0:
            logger.debug("Not pulling data from Nuclia")
            return

        # Lets do pooling from NUA
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
        headers = {}
        data = None
        if self.creds is not None:
            headers["X-STF-NUAKEY"] = f"Bearer {self.creds}"

        if self.onprem:
            url = (
                self.nuclia_public_url.format(zone=self.zone)
                + "/api/v1/processing/pull?partition="
                + self.partition
            )
        else:
            url = (
                self.nuclia_cluster_url
                + "/api/internal/processing/pull?partition="
                + self.partition
            )
        async with aiohttp.ClientSession() as session:
            logger.info(f"Collecting from NucliaDB Cloud {self.partition} partition")
            logger.info(f"{url}")

            while True:
                try:
                    async with session.get(
                        url,
                        headers=headers,
                    ) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            logger.exception(f"Wrong status {resp.status}:{text}")
                            continue
                        try:
                            data = await resp.json()
                        except Exception:
                            text = await resp.text()
                            logger.exception(f"Wrong parsing {resp.status}:{text}")
                            continue

                        if data.get("status") == "ok":
                            check_proxy_telemetry_headers(resp)
                            logger.info(
                                f"Message received from proxy, partition: {self.partition}"
                            )
                            async with self.lock:
                                try:
                                    transaction_utility = get_transaction_utility()
                                    if transaction_utility is None:
                                        raise Exception(
                                            "No transaction utility defined"
                                        )

                                    pb = BrokerMessage()
                                    pb.ParseFromString(
                                        base64.b64decode(data["payload"])
                                    )

                                    logger.debug(
                                        f"Resource: {pb.uuid} KB: {pb.kbid} ProcessingID: {pb.processing_id}"
                                    )

                                    if self.nats_subscriber:
                                        await transaction_utility.commit(
                                            writer=pb, partition=self.partition
                                        )
                                    else:
                                        # No nats defined == monolitic nucliadb
                                        await self.processor.process(
                                            pb,
                                            0,  # Fake sequence id as in local mode there's no transactions
                                            partition=self.partition,
                                            transaction_check=False,
                                        )
                                except Exception as e:
                                    errors.capture_exception(e)
                                    logger.exception(
                                        "Error while pulling and forwarding proxy message to nucliadb nats"
                                    )
                                    raise e
                        elif data.get("status") == "empty":
                            logger_activity.debug(
                                f"No messages waiting in partition #{self.partition}"
                            )
                            await asyncio.sleep(self.pull_time)
                        else:
                            logger.info(f"Proxy pull answered with error: {data}")
                            await asyncio.sleep(self.pull_time)
                except asyncio.exceptions.CancelledError:
                    logger.info(
                        f"Pull task for partition #{self.partition} was canceled, exiting"
                    )
                    raise ReallyStopPulling()

                except ClientConnectorError:
                    logger.error(
                        f"Could not connect to {url}, verify your internet connection"
                    )
                    await asyncio.sleep(self.pull_time)

                except nats.errors.MaxPayloadError as e:
                    if data is not None:
                        logger.error(
                            f"Message too big to transaction: {len(data['payload'])}"
                        )
                    raise e

                except Exception:
                    logger.exception("Gathering changes")
                    await asyncio.sleep(self.pull_time)


class TelemetryHeadersMissing(Exception):
    pass


def check_proxy_telemetry_headers(resp: Response):
    try:
        expected = [
            "x-b3-traceid",
            "x-b3-spanid",
            "x-b3-sampled",
        ]
        missing = [header for header in expected if header not in resp.headers]
        if len(missing) > 0:
            raise TelemetryHeadersMissing(
                f"Missing headers {missing} in proxy response"
            )
    except TelemetryHeadersMissing:
        logger.warning("Some telemetry headers not found in proxy response")
