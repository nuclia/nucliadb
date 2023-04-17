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
import logging
import re
import string
from typing import Optional

from redis import asyncio as aioredis
from redis.asyncio.client import Redis

from nucliadb.ingest.fields.base import Field, FieldTypes
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.search.settings import settings
from nucliadb_telemetry import metrics
from nucliadb_utils.utilities import get_utility, has_feature, set_utility

from .cache import get_resource_from_cache

logger = logging.getLogger(__name__)
PRE_WORD = string.punctuation + " "

CACHE_OPS = metrics.Counter("nucliadb_paragraph_cache_ops", labels={"type": "miss"})
CACHE_HIT_DISTRIBUTION = metrics.Histogram(
    "nucliadb_paragraph_cache_dist",
    buckets=[1, 2, 4, 8, 16, 32, 64, 128, 512, 1024, metrics.INF],
)
GET_PARAGRAPH_LATENCY = metrics.Observer(
    "nucliadb_get_paragraph",
    buckets=[
        0.001,
        0.005,
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1.0,
        2.5,
        metrics.INF,
    ],
    labels={"type": "full"},
)

_PARAGRAPHS_CACHE_UTIL = "paragraphs_cache"


class ParagraphsCache:
    """
    Skeleton of paragraph cache.

    For now, it will be used for us to track hits/misses on a potential
    paragraph cache implementation.
    """

    consumer_task: Optional[asyncio.Task] = None
    redis: Redis

    def __init__(self):
        self.queue = asyncio.Queue()

    async def initialize(self) -> None:
        if (
            settings.search_cache_redis_host is None
            or settings.search_cache_redis_port is None
        ):
            # Cache is not configured, ignore
            return
        self.consumer_task = asyncio.create_task(self.queue_consumer())
        self.redis = aioredis.from_url(
            f"redis://{settings.search_cache_redis_host}:{settings.search_cache_redis_port}"
        )

    async def finalize(self) -> None:
        if self.consumer_task is None:
            return
        self.consumer_task.cancel()
        await self.redis.close(close_connection_pool=True)

    async def queue_consumer(self) -> None:
        while True:
            try:
                key = await self.queue.get()
                key = key + "_hits"
                val = await self.redis.get(key)
                if val is None:
                    CACHE_OPS.inc({"type": "miss"})
                else:
                    CACHE_OPS.inc({"type": "hit"})
                    CACHE_HIT_DISTRIBUTION.observe(int(val))
                await self.redis.incr(key, 1)
                await self.redis.expire(key, 60 * 60)
                self.queue.task_done()
            except (
                asyncio.CancelledError,
                asyncio.TimeoutError,
                RuntimeError,
            ):
                return
            except Exception:  # pragma: no cover
                logger.exception("Error in queue consumer, retrying...")
                await asyncio.sleep(1)

    async def get(
        self,
        *,
        kbid: str,
        rid: str,
        field: str,
        field_id: str,
        start: int,
        end: int,
        split: Optional[str],
    ) -> Optional[str]:
        if self.consumer_task is None:
            return None
        key = f"{kbid}/{rid}/{field}/{field_id}::{start}-{end}:{split or ''}"
        self.queue.put_nowait(key)
        return None


async def initialize_cache() -> None:
    paragraphs_cache = ParagraphsCache()
    await paragraphs_cache.initialize()
    set_utility(_PARAGRAPHS_CACHE_UTIL, paragraphs_cache)


@GET_PARAGRAPH_LATENCY.wrap({"type": "full"})
async def get_paragraph_from_full_text(
    *, field: Field, start: int, end: int, split: Optional[str] = None
) -> str:
    """
    Pull paragraph from full text stored in database.

    This requires downloading the full text and then slicing it.
    """
    extracted_text = await field.get_extracted_text()
    if extracted_text is None:
        logger.warning(f"{field} extracted_text does not exist on DB")
        return ""

    if split not in (None, ""):
        text = extracted_text.split_text[split]  # type: ignore
        return text[start:end]
    else:
        return extracted_text.text[start:end]


@GET_PARAGRAPH_LATENCY.wrap({"type": "seek"})
async def get_paragraph_text_by_seeking(*, field: Field, start: int, end: int) -> str:
    """
    Instead of pulling the full text and slicing it, this function
    seeks directly to the paragraph.

    Text is stored in a predictable protobuf message format that we can still seek to:

    >>> ExtractedText(text='text').SerializeToString()[2:]
    b'text'

    We can NOT support split_text with this approach.
    """
    # seek directly to paragraph
    storage_field = field.get_storage_field(FieldTypes.FIELD_TEXT)
    data = b""
    async for chunk in storage_field.read_range(start + 2, end + 2):
        data += chunk
    return data.decode("utf-8", "ignore")


async def get_paragraph_text(
    *,
    kbid: str,
    rid: str,
    field: str,
    start: int,
    end: int,
    split: Optional[str] = None,
    highlight: bool = False,
    ematches: Optional[list[str]] = None,
    matches: Optional[list[str]] = None,
    orm_resource: Optional[
        ResourceORM
    ] = None,  # allow passing in orm_resource to avoid extra DB calls or txn issues
) -> str:
    if orm_resource is None:
        orm_resource = await get_resource_from_cache(kbid, rid)
        if orm_resource is None:
            logger.error(f"{kbid}/{rid}:{field} does not exist on DB")
            return ""

    _, field_type, field = field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)

    paragraphs_cache: ParagraphsCache = get_utility(_PARAGRAPHS_CACHE_UTIL)
    cache_val = (
        paragraphs_cache is not None
        and await paragraphs_cache.get(
            kbid=kbid,
            rid=rid,
            field=field,
            field_id=field_obj.id,
            start=start,
            end=end,
            split=split,
        )
        or None
    )
    if cache_val is not None:
        return cache_val

    if split in (None, "") and has_feature("nucliadb_seek_to_paragraph"):
        text = await get_paragraph_text_by_seeking(
            field=field_obj, start=start, end=end
        )
    else:
        text = await get_paragraph_from_full_text(
            field=field_obj, start=start, end=end, split=split
        )

    if highlight:
        text = highlight_paragraph(text, words=matches, ematches=ematches)
    return text


async def get_text_sentence(
    rid: str,
    field_type: str,
    field: str,
    kbid: str,
    index: int,
    start: int,
    end: int,
    split: Optional[str] = None,
) -> str:
    """
    Leave separated from get paragraph for now until we understand the differences
    better.
    """
    orm_resource = await get_resource_from_cache(kbid, rid)

    if orm_resource is None:
        logger.warning(f"{rid} does not exist on DB")
        return ""

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.info(
            f"{rid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return ""
    start = start - 1
    if start < 0:
        start = 0
    if split not in (None, ""):
        text = extracted_text.split_text[split]
        splitted_text = text[start:end]
    else:
        splitted_text = extracted_text.text[start:end]
    return splitted_text


def get_regex(some_string: str) -> str:
    return r"\b" + some_string.lower() + r"\b"


def highlight_paragraph(
    text: str, words: Optional[list[str]] = None, ematches: Optional[list[str]] = None
) -> str:
    text_lower = text.lower()

    marks = [0] * (len(text_lower) + 1)
    if ematches is not None:
        for quote in ematches:
            quote_regex = get_regex(quote.lower())
            try:
                for match in re.finditer(quote_regex, text_lower):
                    start, end = match.span()
                    marks[start] = 1
                    marks[end] = 2
            except re.error:
                logger.warning(
                    f"Regex errors while highlighting text. Regex: {quote_regex}"
                )
                continue

    words = words or []
    for word in words:
        word_regex = get_regex(word.lower())
        try:
            for match in re.finditer(word_regex, text_lower):
                start, end = match.span()
                if marks[start] == 0 and marks[end] == 0:
                    marks[start] = 1
                    marks[end] = 2
        except re.error:
            logger.warning(f"Regex errors while highlighting text. Regex: {word_regex}")
            continue

    new_text = ""
    actual = 0
    mod = 0
    skip = False

    length = len(text)

    for index, pos in enumerate(marks):
        if skip:
            skip = False
            continue
        if (index - mod) >= length:
            char_pos = ""
        else:
            begining = True
            if index > 0 and text[index - mod - 1] not in PRE_WORD:
                begining = False
            char_pos = text[index - mod]
            if text[index - mod].lower() != text_lower[index]:
                # May be incorrect positioning due to unicode lower
                mod += 1
                skip = True
        if pos == 1 and actual == 0 and begining:
            new_text += "<mark>"
            new_text += char_pos
            actual = 1
        elif pos == 2 and actual == 1:
            new_text += "</mark>"
            new_text += char_pos
            actual = 0
        elif pos == 1 and actual > 0:
            new_text += char_pos
            actual += 1
        elif pos == 2 and actual > 1:
            new_text += char_pos
            actual -= 1
        else:
            new_text += char_pos

    return new_text
