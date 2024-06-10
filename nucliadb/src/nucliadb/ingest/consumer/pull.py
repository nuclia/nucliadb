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

from aiohttp.client_exceptions import ClientConnectorError

from nucliadb.common import datamanagers
from nucliadb.common.http_clients.processing import ProcessingHTTPClient, get_nua_api_id
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest import logger, logger_activity
from nucliadb.ingest.orm.exceptions import ReallyStopPulling
from nucliadb.ingest.orm.processor import Processor
from nucliadb_protos.writer_pb2 import BrokerMessage, BrokerMessageBlobReference
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.settings import nuclia_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.transaction import MaxTransactionSizeExceededError
from nucliadb_utils.utilities import get_storage, get_transaction_utility


class PullWorker:
    """
    The pull worker is responsible for pulling messages from the pull processing
    http endpoint and injecting them into the processing write queue.

    The processing pull endpoint is also described as the "processing proxy" at times.
    """

    def __init__(
        self,
        driver: Driver,
        partition: str,
        storage: Storage,
        pull_time_error_backoff: int,
        pubsub: Optional[PubSubDriver] = None,
        local_subscriber: bool = False,
        pull_time_empty_backoff: float = 5.0,
        pull_api_timeout: int = 60,
    ):
        self.partition = partition
        self.pull_time_error_backoff = pull_time_error_backoff
        self.pull_time_empty_backoff = pull_time_empty_backoff
        self.pull_api_timeout = pull_api_timeout
        self.local_subscriber = local_subscriber

        self.processor = Processor(driver, storage, pubsub, partition)

    def __str__(self) -> str:
        return f"PullWorker(partition={self.partition})"

    def __repr__(self) -> str:
        return str(self)

    async def handle_message(self, payload: str) -> None:
        pb = BrokerMessage()
        data = base64.b64decode(payload)
        pb.ParseFromString(data)

        logger.debug(f"Resource: {pb.uuid} KB: {pb.kbid} ProcessingID: {pb.processing_id}")

        if not self.local_subscriber:
            transaction_utility = get_transaction_utility()
            if transaction_utility is None:
                raise Exception("No transaction utility defined")
            try:
                await transaction_utility.commit(
                    writer=pb,
                    partition=int(self.partition),
                    # send to separate processor
                    target_subject=const.Streams.INGEST_PROCESSED.subject,
                )
            except MaxTransactionSizeExceededError:
                storage = await get_storage()
                stored_key = await storage.set_stream_message(kbid=pb.kbid, rid=pb.uuid, data=data)
                referenced_pb = BrokerMessageBlobReference(
                    uuid=pb.uuid, kbid=pb.kbid, storage_key=stored_key
                )
                await transaction_utility.commit(
                    writer=referenced_pb,
                    partition=int(self.partition),
                    # send to separate processor
                    target_subject=const.Streams.INGEST_PROCESSED.subject,
                    headers={"X-MESSAGE-TYPE": "PROXY"},
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
        if nuclia_settings.nuclia_service_account is not None:
            headers["X-STF-NUAKEY"] = f"Bearer {nuclia_settings.nuclia_service_account}"
            # parse jwt sub to get pull type id
            try:
                pull_type_id = get_nua_api_id()
            except Exception as exc:
                logger.exception("Could not read NUA API Key. Can not start pull worker")
                raise ReallyStopPulling() from exc
        else:
            pull_type_id = "main"

        async with ProcessingHTTPClient() as processing_http_client:
            logger.info(f"Collecting from NucliaDB Cloud {self.partition} partition")
            while True:
                try:
                    async with datamanagers.with_ro_transaction() as txn:
                        cursor = await datamanagers.processing.get_pull_offset(
                            txn, pull_type_id=pull_type_id, partition=self.partition
                        )

                    data = await processing_http_client.pull(
                        self.partition,
                        cursor=cursor,
                        timeout=self.pull_api_timeout,
                    )
                    if data.status == "ok":
                        logger.info(
                            "Message received from proxy",
                            extra={"partition": self.partition, "cursor": data.cursor},
                        )
                        try:
                            if data.payload is not None:
                                await self.handle_message(data.payload)
                            for payload in data.payloads:
                                # If using cursors and multiple messages are returned, it will be in the
                                # `payloads` property
                                await self.handle_message(payload)
                        except Exception as e:
                            errors.capture_exception(e)
                            logger.exception("Error while pulling and processing message/s")
                            raise e
                        async with datamanagers.with_transaction() as txn:
                            await datamanagers.processing.set_pull_offset(
                                txn,
                                pull_type_id=pull_type_id,
                                partition=self.partition,
                                offset=data.cursor,
                            )
                            await txn.commit()
                    elif data.status == "empty":
                        logger_activity.debug(f"No messages waiting in partition #{self.partition}")
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
                    logger.info(f"Pull task for partition #{self.partition} was canceled, exiting")
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
