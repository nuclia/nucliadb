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
from typing import List, Optional

import aiohttp
import nats
from aiohttp.client_exceptions import ClientConnectorError
from aiohttp.web import Response
from nats.aio.subscription import Subscription
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest import logger, logger_activity
from nucliadb.ingest.maindb.driver import Driver
from nucliadb.ingest.orm.exceptions import ReallyStopPulling
from nucliadb.ingest.orm.processor import Processor
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_transaction_utility, has_feature


class PullWorker:
    """
    The pull worker is responsible for pulling messages from the pull processing
    http endpoint and injecting them into the processing write queue.

    The processing pull endpoint is also described as the "processing proxy" at times.
    """

    subscriptions: List[Subscription]

    def __init__(
        self,
        driver: Driver,
        partition: str,
        storage: Storage,
        pull_time_error_backoff: int,
        zone: str,
        nuclia_cluster_url: str,
        nuclia_public_url: str,
        audit: Optional[AuditStorage],
        onprem: bool,
        cache: Optional[Cache] = None,
        creds: Optional[str] = None,
        local_subscriber: bool = False,
        pull_time_empty_backoff: float = 5.0,
    ):
        self.driver = driver
        self.partition = partition
        self.storage = storage
        self.pull_time_error_backoff = pull_time_error_backoff
        self.pull_time_empty_backoff = pull_time_empty_backoff
        self.audit = audit
        self.zone = zone
        self.nuclia_cluster_url = nuclia_cluster_url
        self.nuclia_public_url = nuclia_public_url
        self.local_subscriber = local_subscriber
        self.creds = creds
        self.cache = cache
        self.onprem = onprem
        self.nc = None

        self.lock = asyncio.Lock()
        self.processor = Processor(driver, storage, audit, cache, partition)

    async def handle_message(self, payload: bytes):
        pb = BrokerMessage()
        pb.ParseFromString(base64.b64decode(payload))

        logger.debug(
            f"Resource: {pb.uuid} KB: {pb.kbid} ProcessingID: {pb.processing_id}"
        )

        if not self.local_subscriber:
            transaction_utility = get_transaction_utility()
            if transaction_utility is None:
                raise Exception("No transaction utility defined")
            subject = None
            if has_feature(const.Features.SEPARATE_PROCESSED_MESSAGE_WRITES):
                # send messsages to queue that is managed by separated consumer
                subject = const.Streams.INGEST_PROCESSED.subject
            await transaction_utility.commit(
                writer=pb, partition=int(self.partition), target_subject=subject
            )
        else:
            # No nats defined == monolitic nucliadb
            await self.processor.process(
                pb,
                0,  # Fake sequence id as in local mode there's no transactions
                partition=self.partition,
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
                                    await self.handle_message(data["payload"])
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
                            await asyncio.sleep(self.pull_time_empty_backoff)
                        else:
                            logger.info(f"Proxy pull answered with error: {data}")
                            await asyncio.sleep(self.pull_time_error_backoff)
                except (
                    asyncio.exceptions.CancelledError,
                    RuntimeError,
                    KeyboardInterrupt,
                    SystemExit,
                ):
                    logger.info(
                        f"Pull task for partition #{self.partition} was canceled, exiting"
                    )
                    raise ReallyStopPulling()

                except ClientConnectorError:
                    logger.error(
                        f"Could not connect to {url}, verify your internet connection"
                    )
                    await asyncio.sleep(self.pull_time_error_backoff)

                except nats.errors.MaxPayloadError as e:
                    if data is not None:
                        logger.error(
                            f"Message too big to transaction: {len(data['payload'])}"
                        )
                    raise e

                except Exception:
                    logger.exception("Gathering changes")
                    await asyncio.sleep(self.pull_time_error_backoff)


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
