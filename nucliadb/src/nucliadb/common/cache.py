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

import contextlib
import logging
from abc import ABC, abstractmethod
from contextvars import ContextVar
from dataclasses import dataclass
from functools import cached_property
from typing import Generic, Optional, TypeVar

import backoff
from async_lru import _LRUCacheWrapper, alru_cache
from typing_extensions import ParamSpec

from nucliadb.common.ids import FieldId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import FieldTypes
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_telemetry.metrics import Counter, Gauge
from nucliadb_utils.utilities import get_storage

logger = logging.getLogger(__name__)

# specific metrics per cache type
cached_resources = Gauge("nucliadb_cached_resources")
cached_extracted_texts = Gauge("nucliadb_cached_extracted_texts")
resource_cache_ops = Counter("nucliadb_resource_cache_ops", labels={"type": ""})
extracted_text_cache_ops = Counter("nucliadb_extracted_text_cache_ops", labels={"type": ""})


K = ParamSpec("K")
T = TypeVar("T")


@dataclass
class CacheMetrics:
    _cache_size: Gauge
    ops: Counter


class Cache(Generic[K, T], ABC):
    """Low-level bounded cache implementation with access to per-key async locks
    in case cache users want to lock concurrent access.

    This cache is measured using a mandatory metric all subclasses must define.

    """

    cache: _LRUCacheWrapper[Optional[T]]

    async def get(self, *args: K.args, **kwargs: K.kwargs) -> Optional[T]:
        result = await self.cache(*args)
        # Do not cache None
        if result is None:
            self.cache.cache_invalidate(*args)
        return result

    def finalize(self):
        info = self.cache.cache_info()
        self.metrics.ops.inc({"type": "miss"}, value=info.misses)
        self.metrics.ops.inc({"type": "hit"}, value=info.hits)

    @abstractmethod
    @cached_property
    def metrics(self) -> CacheMetrics: ...


class ResourceCache(Cache[[str, str], ResourceORM]):
    def __init__(self, cache_size: int) -> None:
        @alru_cache(maxsize=cache_size)
        async def _get_resource(kbid: str, rid: str) -> Optional[ResourceORM]:
            storage = await get_storage()
            async with get_driver().ro_transaction() as txn:
                kb = KnowledgeBoxORM(txn, storage, kbid)
                return await kb.get(rid)

        self.cache = _get_resource

    metrics = CacheMetrics(
        _cache_size=cached_resources,
        ops=resource_cache_ops,
    )


class ExtractedTextCache(Cache[[str, FieldId], ExtractedText]):
    """
    Used to cache extracted text from a resource in memory during the process
    of search results hydration.

    This is needed to avoid fetching the same extracted text multiple times,
    as matching text blocks are processed in parallel and the extracted text is
    fetched for each field where the text block is found.
    """

    def __init__(self, cache_size: int) -> None:
        @alru_cache(maxsize=cache_size)
        @backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
        async def _get_extracted_text(kbid: str, field_id: FieldId) -> Optional[ExtractedText]:
            storage = await get_storage()
            try:
                sf = storage.file_extracted(
                    kbid, field_id.rid, field_id.type, field_id.key, FieldTypes.FIELD_TEXT.value
                )
                return await storage.download_pb(sf, ExtractedText)
            except Exception:
                logger.warning(
                    "Error getting extracted text for field. Retrying",
                    exc_info=True,
                    extra={
                        "kbid": kbid,
                        "resource_id": field_id.rid,
                        "field": f"{field_id.type}/{field_id.key}",
                    },
                )
                raise

        self.cache = _get_extracted_text

    metrics = CacheMetrics(
        _cache_size=cached_extracted_texts,
        ops=extracted_text_cache_ops,
    )


# Global caches (per asyncio task)

rcache: ContextVar[Optional[ResourceCache]] = ContextVar("rcache", default=None)
etcache: ContextVar[Optional[ExtractedTextCache]] = ContextVar("etcache", default=None)


# Cache management


def get_resource_cache() -> Optional[ResourceCache]:
    return rcache.get()


def get_extracted_text_cache() -> Optional[ExtractedTextCache]:
    return etcache.get()


@contextlib.contextmanager
def _use_cache(klass: type[Cache], context_var: ContextVar, /, **kwargs):
    """Context manager that manages a context var cache. It's responsible of
    cache creation and cleanup.

    Note the configured cache is specific to the current asyncio task (and all
    its subtasks). If you spawn subtasks that should share a cache, make sure
    the parent task is the one using this decorator, otherwise, each subtask
    will use its own independent cache instance

    Do not use the cache object outside the scope of this context manager!
    Otherwise, metrics and cleanup could get wrong.

    """
    cache = klass(**kwargs)
    token = context_var.set(cache)
    try:
        yield cache
    finally:
        context_var.reset(token)
        cache.finalize()


@contextlib.contextmanager
def resource_cache(size: int):
    with _use_cache(ResourceCache, rcache, cache_size=size) as cache:
        yield cache


@contextlib.contextmanager
def extracted_text_cache(size: int):
    with _use_cache(ExtractedTextCache, etcache, cache_size=size) as cache:
        yield cache
