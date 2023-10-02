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

from nucliadb_protos.utils_pb2 import ExtractedText

from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb_telemetry import metrics

from .cache import get_resource_from_cache

logger = logging.getLogger(__name__)
PRE_WORD = string.punctuation + " "

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


EXTRACTED_CACHE_OPS = metrics.Counter(
    "nucliadb_extracted_text_cache_ops", labels={"type": ""}
)


class ExtractedTextCache:
    """
    Used to cache extracted text from a resource in memory during
    the process of search results serialization.
    """

    def __init__(self):
        self.locks = {}
        self.values = {}

    def get_value(self, key: str) -> Optional[ExtractedText]:
        return self.values.get(key)

    def get_lock(self, key: str) -> asyncio.Lock:
        return self.locks.setdefault(key, asyncio.Lock())

    def set_value(self, key: str, value: ExtractedText) -> None:
        self.values[key] = value

    def clear(self):
        self.values.clear()
        self.locks.clear()


async def get_field_extracted_text(
    field: Field, cache: Optional[ExtractedTextCache] = None
) -> Optional[ExtractedText]:
    if cache is None:
        return await field.get_extracted_text()

    key = f"{field.kbid}/{field.uuid}/{field.id}"
    extracted_text = cache.get_value(key)
    if extracted_text is not None:
        EXTRACTED_CACHE_OPS.inc({"type": "hit"})
        return extracted_text

    async with cache.get_lock(key):
        # Check again in case another task already fetched it
        extracted_text = cache.get_value(key)
        if extracted_text is not None:
            EXTRACTED_CACHE_OPS.inc({"type": "hit"})
            return extracted_text

        EXTRACTED_CACHE_OPS.inc({"type": "miss"})
        extracted_text = await field.get_extracted_text()
        if extracted_text is not None:
            # Only cache if we actually have extracted text
            cache.set_value(key, extracted_text)
        return extracted_text


@GET_PARAGRAPH_LATENCY.wrap({"type": "full"})
async def get_paragraph_from_full_text(
    *,
    field: Field,
    start: int,
    end: int,
    split: Optional[str] = None,
    extracted_text_cache: Optional[ExtractedTextCache] = None,
) -> str:
    """
    Pull paragraph from full text stored in database.

    This requires downloading the full text and then slicing it.
    """
    extracted_text = await get_field_extracted_text(field, cache=extracted_text_cache)
    if extracted_text is None:
        logger.warning(f"{field} extracted_text does not exist on DB yet")
        return ""

    if split not in (None, ""):
        text = extracted_text.split_text[split]  # type: ignore
        return text[start:end]
    else:
        return extracted_text.text[start:end]


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
    extracted_text_cache: Optional[ExtractedTextCache] = None,
) -> str:
    if orm_resource is None:
        orm_resource = await get_resource_from_cache(kbid, rid)
        if orm_resource is None:
            logger.error(f"{kbid}/{rid}:{field} does not exist on DB")
            return ""

    _, field_type, field = field.split("/")
    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)

    text = await get_paragraph_from_full_text(
        field=field_obj,
        start=start,
        end=end,
        split=split,
        extracted_text_cache=extracted_text_cache,
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
