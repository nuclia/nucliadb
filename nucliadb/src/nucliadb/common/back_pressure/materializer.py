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
import logging
import threading
from typing import Optional

from cachetools import TTLCache
from fastapi import HTTPException

from nucliadb.common.back_pressure.cache import cached_back_pressure
from nucliadb.common.back_pressure.settings import settings
from nucliadb.common.back_pressure.utils import (
    BackPressureData,
    BackPressureException,
    estimate_try_after,
    get_nats_consumer_pending_messages,
    is_back_pressure_enabled,
)
from nucliadb.common.context import ApplicationContext
from nucliadb.common.http_clients.processing import ProcessingHTTPClient
from nucliadb_telemetry import metrics
from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import is_onprem_nucliadb

logger = logging.getLogger(__name__)


back_pressure_observer = metrics.Observer("nucliadb_back_pressure", labels={"type": ""})


class BackPressureMaterializer:
    """
    Singleton class that will run in the background gathering the different
    stats to apply back pressure and materializing it in memory. This allows us
    to do stale-reads when checking if back pressure is needed for a particular
    request - thus not slowing it down.
    """

    def __init__(
        self,
        nats_manager: NatsConnectionManager,
        indexing_check_interval: int = 30,
        ingest_check_interval: int = 30,
    ):
        self.nats_manager = nats_manager
        self.processing_http_client = ProcessingHTTPClient()

        self.indexing_check_interval = indexing_check_interval
        self.ingest_check_interval = ingest_check_interval

        self.ingest_pending: int = 0
        self.indexing_pending: int = 0

        self._tasks: list[asyncio.Task] = []
        self._running = False

        self.processing_pending_cache = TTLCache(maxsize=1024, ttl=60)  # type: ignore
        self.processing_pending_locks: dict[str, asyncio.Lock] = {}

    async def start(self):
        self._tasks.append(asyncio.create_task(self._get_indexing_pending_task()))
        self._tasks.append(asyncio.create_task(self._get_ingest_pending_task()))
        self._running = True

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        await self.processing_http_client.close()
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def get_processing_pending(self, kbid: str) -> int:
        """
        We don't materialize the pending messages for every kbid, but values are cached for some time.
        """
        cached = self.processing_pending_cache.get(kbid)
        if cached is not None:
            return cached

        lock = self.processing_pending_locks.setdefault(kbid, asyncio.Lock())
        async with lock:
            # Check again if the value has been cached while we were waiting for the lock
            cached = self.processing_pending_cache.get(kbid)
            if cached is not None:
                return cached

            # Get the pending messages and cache the result
            try:
                with back_pressure_observer({"type": "get_processing_pending"}):
                    pending = await self._get_processing_pending(kbid)
            except Exception:  # pragma: no cover
                # Do not cache if there was an error
                logger.exception(
                    "Error getting pending messages to process. Back pressure on proccessing for KB can't be applied.",
                    exc_info=True,
                    extra={"kbid": kbid},
                )
                return 0

            if pending > 0:
                logger.info(
                    f"Processing returned {pending} pending messages for KB",
                    extra={"kbid": kbid},
                )
            self.processing_pending_cache[kbid] = pending
            return pending

    async def _get_processing_pending(self, kbid: str) -> int:
        response = await self.processing_http_client.stats(kbid=kbid, timeout=0.5)
        return response.incomplete

    def get_indexing_pending(self) -> int:
        return self.indexing_pending

    def get_ingest_pending(self) -> int:
        return self.ingest_pending

    async def _get_indexing_pending_task(self):
        try:
            while True:
                try:
                    with back_pressure_observer({"type": "get_indexing_pending"}):
                        self.indexing_pending = await get_nats_consumer_pending_messages(
                            self.nats_manager,
                            stream="nidx",
                            consumer="nidx",
                        )
                except Exception:  # pragma: no cover
                    logger.exception(
                        "Error getting pending messages to index",
                        exc_info=True,
                    )
                await asyncio.sleep(self.indexing_check_interval)
        except asyncio.CancelledError:
            pass

    async def _get_ingest_pending_task(self):
        try:
            while True:
                try:
                    with back_pressure_observer({"type": "get_ingest_pending"}):
                        self.ingest_pending = await get_nats_consumer_pending_messages(
                            self.nats_manager,
                            stream=const.Streams.INGEST_PROCESSED.name,
                            consumer=const.Streams.INGEST_PROCESSED.group,
                        )
                except Exception:  # pragma: no cover
                    logger.exception(
                        "Error getting pending messages to ingest",
                        exc_info=True,
                    )
                await asyncio.sleep(self.ingest_check_interval)
        except asyncio.CancelledError:
            pass

    def check_indexing(self):
        max_pending = settings.max_indexing_pending
        if max_pending <= 0:
            # Indexing back pressure is disabled
            return
        pending = self.get_indexing_pending()
        if pending > max_pending:
            try_after = estimate_try_after(
                rate=settings.indexing_rate,
                pending=pending,
                max_wait=settings.max_wait_time,
            )
            data = BackPressureData(type="indexing", try_after=try_after)
            raise BackPressureException(data)

    def check_ingest(self):
        max_pending = settings.max_ingest_pending
        if max_pending <= 0:
            # Ingest back pressure is disabled
            return
        ingest_pending = self.get_ingest_pending()
        if ingest_pending > max_pending:
            try_after = estimate_try_after(
                rate=settings.ingest_rate,
                pending=ingest_pending,
                max_wait=settings.max_wait_time,
            )
            data = BackPressureData(type="ingest", try_after=try_after)
            raise BackPressureException(data)

    async def check_processing(self, kbid: str):
        max_pending = settings.max_processing_pending
        if max_pending <= 0:
            # Processing back pressure is disabled
            return

        kb_pending = await self.get_processing_pending(kbid)
        if kb_pending > max_pending:
            try_after = estimate_try_after(
                rate=settings.processing_rate,
                pending=kb_pending,
                max_wait=settings.max_wait_time,
            )
            data = BackPressureData(type="processing", try_after=try_after)
            raise BackPressureException(data)


MATERIALIZER: Optional[BackPressureMaterializer] = None
materializer_lock = threading.Lock()


async def start_materializer(context: ApplicationContext):
    global MATERIALIZER
    if MATERIALIZER is not None:
        logger.warning("BackPressureMaterializer already started")
        return
    with materializer_lock:
        if MATERIALIZER is not None:
            return
        logger.info("Initializing materializer")
        try:
            nats_manager = context.nats_manager
        except AttributeError:
            logger.warning(
                "Could not initialize materializer. Nats manager not found or not initialized yet"
            )
            return
        materializer = BackPressureMaterializer(
            nats_manager,
            indexing_check_interval=settings.indexing_check_interval,
            ingest_check_interval=settings.ingest_check_interval,
        )
        await materializer.start()
        MATERIALIZER = materializer


async def stop_materializer():
    global MATERIALIZER
    if MATERIALIZER is None or not MATERIALIZER.running:
        logger.warning("BackPressureMaterializer already stopped")
        return
    with materializer_lock:
        if MATERIALIZER is None:
            return
        logger.info("Stopping materializer")
        await MATERIALIZER.stop()
        MATERIALIZER = None


def get_materializer() -> BackPressureMaterializer:
    global MATERIALIZER
    if MATERIALIZER is None:
        raise RuntimeError("BackPressureMaterializer not initialized")
    return MATERIALIZER


async def maybe_back_pressure(kbid: str, resource_uuid: Optional[str] = None) -> None:
    """
    This function does system checks to see if we need to put back pressure on writes.
    In that case, a HTTP 429 will be raised with the estimated time to try again.
    """
    if not is_back_pressure_enabled() or is_onprem_nucliadb():
        return
    await back_pressure_checks(kbid, resource_uuid)


async def back_pressure_checks(kbid: str, resource_uuid: Optional[str] = None):
    """
    Will raise a 429 if back pressure is needed:
    - If the processing engine is behind.
    - If ingest processed consumer is behind.
    - If the indexing on nodes affected by the request (kbid, and resource_uuid) is behind.
    """
    materializer = get_materializer()
    try:
        with cached_back_pressure(f"{kbid}-{resource_uuid}"):
            materializer.check_indexing()
            materializer.check_ingest()
            await materializer.check_processing(kbid)
    except BackPressureException as exc:
        logger.info(
            "Back pressure applied",
            extra={
                "kbid": kbid,
                "resource_uuid": resource_uuid,
                "try_after": exc.data.try_after,
                "back_pressure_type": exc.data.type,
            },
        )
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {exc.data.try_after}",
                "try_after": exc.data.try_after.timestamp(),
                "back_pressure_type": exc.data.type,
            },
        ) from exc
