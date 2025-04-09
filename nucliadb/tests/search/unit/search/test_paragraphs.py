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
import random
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb.common.cache import ExtractedTextCache
from nucliadb.common.ids import ParagraphId
from nucliadb.search.search import paragraphs
from nucliadb.search.search.cache import set_extracted_text_cache
from nucliadb_protos.utils_pb2 import ExtractedText


@pytest.fixture()
def extracted_text():
    yield ExtractedText(
        text=b"Hello World!",
        split_text={"1": b"Hello", "2": b"World!"},
    )


@pytest.fixture()
def field(extracted_text):
    mock = MagicMock()
    mock.get_extracted_text = AsyncMock(return_value=extracted_text)
    yield mock


async def test_get_paragraph_from_full_text(field, extracted_text: ExtractedText):
    assert (
        await paragraphs.get_paragraph_from_full_text(field=field, start=0, end=12, split=None)
        == extracted_text.text
    )


async def test_get_paragraph_from_full_text_with_split(field, extracted_text: ExtractedText):
    assert (
        await paragraphs.get_paragraph_from_full_text(field=field, start=0, end=6, split="1")
        == extracted_text.split_text["1"]
    )


class TestGetParagraphText:
    @pytest.fixture()
    def orm_resource(self, field):
        mock = AsyncMock()
        mock.get_field.return_value = field
        with patch(
            "nucliadb.search.search.paragraphs.cache.get_resource",
            return_value=mock,
        ):
            yield mock

    async def test_get_paragraph_text(self, orm_resource):
        assert (
            await paragraphs.get_paragraph_text(
                kbid="kbid",
                paragraph_id=ParagraphId.from_string("rid/t/text/0-12"),
                highlight=True,
                ematches=None,
                matches=None,
            )
            == "Hello World!"
        )

        orm_resource.get_field.assert_called_once_with("text", 4, load=False)


async def fake_get_extracted_text_from_gcloud(*args, **kwargs):
    await asyncio.sleep(random.uniform(0, 1))
    return ExtractedText(text=b"Hello World!")


async def test_get_field_extracted_text_is_cached(field):
    field.kbid = "kbid"
    field.uuid = "rid"
    field.id = "fid"
    # Simulate a slow response from GCloud
    field.get_extracted_text = AsyncMock(side_effect=fake_get_extracted_text_from_gcloud)

    # Run 10 times in parallel to check that the cache is working
    set_extracted_text_cache()
    futures = [paragraphs.cache.get_field_extracted_text(field) for _ in range(10)]
    await asyncio.gather(*futures)

    field.get_extracted_text.assert_awaited_once()


async def test_get_field_extracted_text_is_not_cached_when_none(field):
    field.get_extracted_text = AsyncMock(return_value=None)

    await paragraphs.cache.get_field_extracted_text(field)
    await paragraphs.cache.get_field_extracted_text(field)

    assert field.get_extracted_text.await_count == 2


def test_extracted_text_cache():
    etcache = ExtractedTextCache()
    assert etcache.get("foo") is None

    assert isinstance(etcache.get_lock("foo"), asyncio.Lock)
    assert len(etcache.locks) == 1

    etcache.set("foo", "bar")
    assert len(etcache.values) == 1

    assert etcache.get("foo") == "bar"

    etcache.clear()

    assert len(etcache.values) == 0
    assert len(etcache.locks) == 0
