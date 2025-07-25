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
from unittest.mock import AsyncMock, MagicMock

import pytest

from nucliadb.ingest.orm.brain_v2 import ResourceBrain
from nucliadb.ingest.orm.resource import (
    Resource,
    get_file_page_positions,
    get_text_field_mimetype,
    maybe_update_basic_icon,
    maybe_update_basic_summary,
    maybe_update_basic_thumbnail,
    update_basic_languages,
)
from nucliadb_protos import utils_pb2
from nucliadb_protos.resources_pb2 import (
    AllFieldIDs,
    Basic,
    CloudFile,
    FieldID,
    FieldText,
    FieldType,
    FileExtractedData,
    PagePositions,
)
from nucliadb_protos.writer_pb2 import BrokerMessage


async def test_get_file_page_positions():
    extracted_data = FileExtractedData()
    extracted_data.file_pages_previews.positions.extend(
        [PagePositions(start=0, end=10), PagePositions(start=11, end=20)]
    )
    file_field = AsyncMock(get_file_extracted_data=AsyncMock(return_value=extracted_data))
    assert await get_file_page_positions(file_field) == {0: (0, 10), 1: (11, 20)}


@pytest.mark.parametrize(
    "basic,summary,updated",
    [
        (Basic(), "new_summary", True),
        (Basic(summary="summary"), "new_summary", False),
        (Basic(summary="summary"), "", False),
    ],
)
def test_maybe_update_basic_summary(basic, summary, updated):
    assert maybe_update_basic_summary(basic, summary) is updated
    if updated:
        assert basic.summary == summary
    else:
        assert basic.summary != summary


def test_update_basic_languages():
    basic = Basic()
    # Languages are updated the first time
    assert update_basic_languages(basic, ["en"]) is True
    assert basic.metadata.language == "en"
    assert basic.metadata.languages == ["en"]

    # Languages are not updated
    assert update_basic_languages(basic, ["en"]) is False
    assert basic.metadata.language == "en"
    assert basic.metadata.languages == ["en"]

    # Main language is not updated but new language is added
    assert update_basic_languages(basic, ["de"]) is True
    assert basic.metadata.language == "en"
    assert basic.metadata.languages == ["en", "de"]

    # Null values
    assert update_basic_languages(basic, [""]) is False
    assert update_basic_languages(basic, [None]) is False  # type: ignore
    assert basic.metadata.language == "en"
    assert basic.metadata.languages == ["en", "de"]


@pytest.mark.parametrize(
    "basic,thumbnail,updated",
    [
        (Basic(), CloudFile(uri="new_thumbnail_url"), True),
        (
            Basic(thumbnail="old_thumbnail_url"),
            CloudFile(uri="new_thumbnail_url"),
            False,
        ),
        (Basic(thumbnail="old_thumbnail_url"), None, False),
    ],
)
def test_maybe_update_basic_thumbnail(basic, thumbnail, updated):
    assert maybe_update_basic_thumbnail(basic, thumbnail, "kbid") == updated
    if updated:
        assert basic.thumbnail == thumbnail.uri
    else:
        assert basic.thumbnail == "old_thumbnail_url"


def test_maybe_update_basic_thumbnail_replaces_kbid():
    basic = Basic()
    thumbnail = CloudFile(uri="/kb/old_kbid/thumbnail.png")
    kbid = "new_kbid"
    maybe_update_basic_thumbnail(basic, thumbnail, kbid)
    assert basic.thumbnail == "/kb/new_kbid/thumbnail.png"


@pytest.mark.parametrize(
    "text_format,mimetype",
    [
        (None, None),
        (FieldText.Format.PLAIN, "text/plain"),
        (FieldText.Format.HTML, "text/html"),
        (FieldText.Format.RST, "text/x-rst"),
        (FieldText.Format.MARKDOWN, "text/markdown"),
        (FieldText.Format.KEEP_MARKDOWN, "text/markdown"),
        (FieldText.Format.JSON, "application/json"),
    ],
)
def test_get_text_field_mimetype(text_format, mimetype):
    message = BrokerMessage()
    if text_format is not None:
        message.texts["foo"].body = "foo"
        message.texts["foo"].format = text_format
    assert get_text_field_mimetype(message) == mimetype


@pytest.mark.parametrize(
    "basic,icon,updated",
    [
        (Basic(), None, False),
        (Basic(icon="text/plain"), "text/html", False),
        (Basic(), "text/html", True),
        (Basic(icon=""), "text/html", True),
        (Basic(icon="application/octet-stream"), "text/html", True),
        # Invalid icon should not be updated
        (Basic(), "invalid", False),
    ],
)
def test_maybe_update_basic_icon(basic, icon, updated):
    assert maybe_update_basic_icon(basic, icon) == updated
    if updated:
        assert basic.icon == icon
    else:
        assert basic.icon != icon


class Transaction:
    def __init__(self):
        self.kv = {}

    async def get(self, key, **kwargs):
        return self.kv.get(key)

    async def set(self, key, value):
        self.kv[key] = value


@pytest.fixture(scope="function")
def txn():
    return Transaction()


@pytest.fixture(scope="function")
def storage():
    mock = AsyncMock()
    return mock


@pytest.fixture(scope="function")
def kb():
    mock = AsyncMock()
    mock.kbid = "mock-kbid"
    return mock


async def test_get_fields_ids_caches_keys(txn, storage, kb):
    resource = Resource(txn, storage, kb, "rid")
    cached_field_keys = [(0, "foo"), (1, "bar")]
    new_field_keys = [(2, "baz")]
    resource._inner_get_fields_ids = AsyncMock(return_value=new_field_keys)  # type: ignore
    resource.all_fields_keys = cached_field_keys

    assert await resource.get_fields_ids() == cached_field_keys
    resource._inner_get_fields_ids.assert_not_awaited()

    assert await resource.get_fields_ids(force=True) == new_field_keys
    resource._inner_get_fields_ids.assert_awaited_once()
    assert resource.all_fields_keys == new_field_keys

    # If the all_field_keys is an empty list,
    # we should not be calling the inner_get_fields_ids
    resource.all_fields_keys = []
    resource._inner_get_fields_ids.reset_mock()
    assert await resource.get_fields_ids() == []
    resource._inner_get_fields_ids.assert_not_awaited()


async def test_get_set_all_field_ids(txn, storage, kb):
    resource = Resource(txn, storage, kb, "rid")

    assert await resource.get_all_field_ids(for_update=False) is None

    all_fields = AllFieldIDs()
    all_fields.fields.append(FieldID(field_type=FieldType.TEXT, field="text"))

    await resource.set_all_field_ids(all_fields)

    assert await resource.get_all_field_ids(for_update=False) == all_fields


async def test_update_all_fields_key(txn, storage, kb):
    resource = Resource(txn, storage, kb, "rid")

    await resource.update_all_field_ids(updated=[], deleted=[])

    # Initial value is Empty
    assert (await resource.get_all_field_ids(for_update=False)) == AllFieldIDs()

    all_fields = AllFieldIDs()
    all_fields.fields.append(FieldID(field_type=FieldType.TEXT, field="text1"))
    all_fields.fields.append(FieldID(field_type=FieldType.TEXT, field="text2"))

    await resource.update_all_field_ids(updated=all_fields.fields)

    # Check updates
    assert await resource.get_all_field_ids(for_update=False) == all_fields

    file_field = FieldID(field_type=FieldType.FILE, field="file")
    await resource.update_all_field_ids(updated=[file_field])

    result = await resource.get_all_field_ids(for_update=False)
    assert list(result.fields) == list(all_fields.fields) + [file_field]

    # Check deletes
    await resource.update_all_field_ids(deleted=[file_field])

    assert await resource.get_all_field_ids(for_update=False) == all_fields


async def test_apply_fields_calls_update_all_field_ids(txn, storage, kb):
    resource = Resource(txn, storage, kb, "rid")
    resource.update_all_field_ids = AsyncMock()  # type: ignore
    resource.set_field = AsyncMock()  # type: ignore

    bm = MagicMock()
    bm.texts = {"text": MagicMock()}
    bm.links = {"link": MagicMock()}
    bm.files = {"file": MagicMock()}
    bm.conversations = {"conversation": MagicMock()}
    bm.delete_fields.append(FieldID(field_type=FieldType.CONVERSATION, field="to_delete"))

    await resource.apply_fields(bm)

    resource.update_all_field_ids.assert_awaited_once()

    resource.update_all_field_ids.call_args[1]["updated"] == [
        FieldID(field_type=FieldType.TEXT, field="text"),
        FieldID(field_type=FieldType.LINK, field="link"),
        FieldID(field_type=FieldType.FILE, field="file"),
        FieldID(field_type=FieldType.CONVERSATION, field="conversation"),
    ]
    resource.update_all_field_ids.call_args[1]["deleted"] == [
        FieldID(field_type=FieldType.CONVERSATION, field="to_delete"),
    ]


async def test_apply_extracted_vectors_cut_by_dimension(txn, storage, kb):
    STORED_VECTOR_DIMENSION = 100
    MATRYOSHKA_DIMENSION = 10

    vectors = utils_pb2.VectorObject(
        vectors=utils_pb2.Vectors(
            vectors=[
                utils_pb2.Vector(
                    start=0,
                    end=10,
                    start_paragraph=0,
                    end_paragraph=10,
                    vector=[1.0] * STORED_VECTOR_DIMENSION,
                )
            ]
        )
    )

    brain = ResourceBrain("rid")
    brain.generate_vectors(
        "t/text",
        vectors,
        vectorset="my-vectorset",
        replace_field=False,
        vector_dimension=STORED_VECTOR_DIMENSION,
    )

    sentences = (
        brain.brain.paragraphs["t/text"]
        .paragraphs["rid/t/text/0-10"]
        .vectorsets_sentences["my-vectorset"]
        .sentences
    )
    assert len(sentences) == 1
    assert sentences["rid/t/text/0/0-10"].metadata.position.start == 0
    assert sentences["rid/t/text/0/0-10"].metadata.position.end == 10
    assert len(sentences["rid/t/text/0/0-10"].vector) == STORED_VECTOR_DIMENSION

    brain = ResourceBrain("rid")
    brain.generate_vectors(
        "t/text",
        vectors,
        vectorset="my-vectorset",
        replace_field=False,
        vector_dimension=MATRYOSHKA_DIMENSION,
    )

    sentences = (
        brain.brain.paragraphs["t/text"]
        .paragraphs["rid/t/text/0-10"]
        .vectorsets_sentences["my-vectorset"]
        .sentences
    )
    assert len(sentences) == 1
    assert sentences["rid/t/text/0/0-10"].metadata.position.start == 0
    assert sentences["rid/t/text/0/0-10"].metadata.position.end == 10
    assert len(sentences["rid/t/text/0/0-10"].vector) == MATRYOSHKA_DIMENSION
