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
from typing import Optional

import nats
import nats.errors
from aiohttp.client_exceptions import ClientConnectorError
from nucliadb_protos.writer_pb2 import BrokerMessage, BrokerMessageBlobReference

from nucliadb.common.http_clients.processing import ProcessingV2HTTPClient
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest import logger
from nucliadb.ingest.orm.exceptions import ReallyStopPulling
from nucliadb.ingest.orm.processor import Processor
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.settings import nuclia_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage, get_transaction_utility

DB_TXN_KEY = "pull/cursor"


class PullWorkerV2:
    """
    V2 of the pull worker that utilizes V2 of the pull API.

    Right now, this is the "simple" way to integrate that works for both on prem and SaaS.

    Longer term, consider integrating with message queue.
    """

    def __init__(
        self,
        driver: Driver,
        storage: Storage,
        pull_time_error_backoff: int,
        pubsub: Optional[PubSubDriver] = None,
        local_subscriber: bool = False,
        pull_time_empty_backoff: float = 5.0,
    ):
        self.driver = driver
        self.pull_time_error_backoff = pull_time_error_backoff
        self.pull_time_empty_backoff = pull_time_empty_backoff
        self.local_subscriber = local_subscriber

        self.processor = Processor(driver, storage, pubsub)

    def __str__(self) -> str:
        return f"PullWorker V2"

    def __repr__(self) -> str:
        return str(self)

    async def handle_message(self, payload: str) -> None:
        pb = BrokerMessage()
        data = base64.b64decode(payload)
        pb.ParseFromString(data)

        logger.debug(
            f"Resource: {pb.uuid} KB: {pb.kbid} ProcessingID: {pb.processing_id}"
        )

        if not self.local_subscriber:
            transaction_utility = get_transaction_utility()
            if transaction_utility is None:
                raise Exception("No transaction utility defined")
            try:
                await transaction_utility.commit(
                    writer=pb,
                    partition=-1,
                    # send to separate processor
                    target_subject=const.Streams.INGEST_PROCESSED.subject,
                )
            except nats.errors.MaxPayloadError:
                storage = await get_storage()
                stored_key = await storage.set_stream_message(
                    kbid=pb.kbid, rid=pb.uuid, data=data
                )
                referenced_pb = BrokerMessageBlobReference(
                    uuid=pb.uuid, kbid=pb.kbid, storage_key=stored_key
                )
                await transaction_utility.commit(
                    writer=referenced_pb,
                    partition=-1,
                    # send to separate processor
                    target_subject=const.Streams.INGEST_PROCESSED.subject,
                    headers={"X-MESSAGE-TYPE": "PROXY"},
                )
        else:
            # No nats defined == monolitic nucliadb
            await self.processor.process(
                pb,
                0,  # Fake sequence id as in local mode there's no transactions
                partition="-1",
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
        if nuclia_settings.nuclia_service_account is not None:
            headers[
                "X-Nuclia-NUAKEY"
            ] = f"Bearer {nuclia_settings.nuclia_service_account}"

        cursor = None
        async with self.driver.transaction() as txn:
            val = await txn.get(DB_TXN_KEY)
            if val is not None:
                cursor = val.decode()

        sleep_backoff = 0.1
        async with ProcessingV2HTTPClient() as processing_http_client:
            logger.info(f"Collecting from NucliaDB Cloud")
            while True:
                try:
                    await asyncio.sleep(sleep_backoff)
                    data = await processing_http_client.pull(cursor=cursor, limit=3)

                    if len(data.results) > 0:
                        for result in data.results:
                            await self.handle_message(result.payload)
                        cursor = data.cursor
                        async with self.driver.transaction() as txn:
                            await txn.set(DB_TXN_KEY, cursor.encode())
                        sleep_backoff = 0.1
                    else:
                        sleep_backoff = self.pull_time_empty_backoff
                except (
                    asyncio.exceptions.CancelledError,
                    RuntimeError,
                    KeyboardInterrupt,
                    SystemExit,
                ):
                    logger.info(f"Pull task for was canceled, exiting")
                    raise ReallyStopPulling()
                except ClientConnectorError:
                    logger.error(
                        f"Could not connect to processing engine, \
                         {processing_http_client.base_url} verify your internet connection"
                    )
                    sleep_backoff = self.pull_time_error_backoff
                except Exception:
                    logger.exception("Unhandled error pulling messages from processing")
                    sleep_backoff = self.pull_time_error_backoff
