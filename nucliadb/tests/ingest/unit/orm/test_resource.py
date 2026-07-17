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
import unittest
import unittest.mock
from unittest.mock import AsyncMock

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.brain_v2 import ResourceBrain
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor.processor import (
    get_text_field_mimetype,
    maybe_update_basic_icon,
    maybe_update_basic_summary,
    maybe_update_basic_thumbnail,
    update_basic_languages,
)
from nucliadb.ingest.orm.resource import (
    Resource,
    get_file_page_positions,
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


@pytest.fixture(scope="function")
async def txn(maindb_driver: Driver):
    async with maindb_driver.rw_transaction() as txn:
        yield txn


@pytest.fixture(scope="function")
def storage():
    mock = AsyncMock()
    return mock


@pytest.fixture(scope="function")
async def kb(maindb_driver: Driver):
    kbid = KnowledgeBox.new_unique_kbid()
    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.kb.kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug=f"slug-{kbid}")
        await txn.commit()
    return kbid


@pytest.fixture(scope="function")
async def rid(maindb_driver: Driver, kb: str):
    r_id = Resource.new_unique_rid()
    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.resources.resources_v2.set_slug(txn, kbid=kb, rid=r_id, slug=f"slug-{r_id}")
        await txn.commit()
    return r_id


@pytest.fixture(scope="function", autouse=True)
def file_md5_mock():
    with unittest.mock.patch("nucliadb.ingest.orm.resource.file_md5", AsyncMock()) as mock:
        yield mock


async def test_get_set_all_field_ids(txn, storage, kb, rid):
    resource = Resource(txn, storage, kb, rid)

    all_field_ids = await resource.get_all_field_ids()
    assert all_field_ids is None or all_field_ids.fields == []

    all_fields = AllFieldIDs()
    all_fields.fields.append(FieldID(field_type=FieldType.TEXT, field="text"))

    await datamanagers.fields.fields_v2.set(
        txn,
        kbid=kb,
        rid=rid,
        field_type="t",
        field_id="text",
        value=FieldText(body="text", format=FieldText.Format.PLAIN),
    )

    final = await resource.get_all_field_ids()
    assert final is not None
    assert final == all_fields


async def test_update_all_fields_key(txn, storage, kb, rid):
    resource = Resource(txn, storage, kb, rid)

    # Initial value is Empty
    assert (await resource.get_all_field_ids()) == AllFieldIDs()

    all_fields = AllFieldIDs()
    all_fields.fields.append(FieldID(field_type=FieldType.TEXT, field="text1"))
    all_fields.fields.append(FieldID(field_type=FieldType.TEXT, field="text2"))
    await datamanagers.fields.fields_v2.set(
        txn,
        kbid=kb,
        rid=rid,
        field_type="t",
        field_id="text1",
        value=FieldText(body="text1", format=FieldText.Format.PLAIN),
    )
    await datamanagers.fields.fields_v2.set(
        txn,
        kbid=kb,
        rid=rid,
        field_type="t",
        field_id="text2",
        value=FieldText(body="text2", format=FieldText.Format.PLAIN),
    )

    # Check updates
    assert await resource.get_all_field_ids() == all_fields

    file_field = FieldID(field_type=FieldType.FILE, field="file")
    await datamanagers.fields.fields_v2.set(
        txn, kbid=kb, rid=rid, field_type="f", field_id="file", value=FileExtractedData()
    )
    result = await resource.get_all_field_ids()
    assert result is not None
    assert len(result.fields) == 3
    # Sort them by field_type and field to ensure consistent ordering for the assertion
    obtained = list(result.fields)
    obtained.sort(key=lambda x: (x.field_type, x.field))
    expected = [*list(all_fields.fields), file_field]
    expected.sort(key=lambda x: (x.field_type, x.field))
    assert obtained == expected

    # Check deletes
    await datamanagers.fields.fields_v2.delete(txn, kbid=kb, rid=rid, field_type="f", field_id="file")

    assert await resource.get_all_field_ids() == all_fields


async def test_apply_extracted_vectors_cut_by_dimension(txn, storage, kb, rid):
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

    brain = ResourceBrain(rid)
    brain.generate_vectors(
        "t/text",
        vectors,
        vectorset="my-vectorset",
        replace_field=False,
        vector_dimension=STORED_VECTOR_DIMENSION,
    )

    sentences = (
        brain.brain.paragraphs["t/text"]
        .paragraphs[f"{rid}/t/text/0-10"]
        .vectorsets_sentences["my-vectorset"]
        .sentences
    )
    assert len(sentences) == 1
    assert sentences[f"{rid}/t/text/0/0-10"].metadata.position.start == 0
    assert sentences[f"{rid}/t/text/0/0-10"].metadata.position.end == 10
    assert len(sentences[f"{rid}/t/text/0/0-10"].vector) == STORED_VECTOR_DIMENSION

    brain = ResourceBrain(rid)
    brain.generate_vectors(
        "t/text",
        vectors,
        vectorset="my-vectorset",
        replace_field=False,
        vector_dimension=MATRYOSHKA_DIMENSION,
    )

    sentences = (
        brain.brain.paragraphs["t/text"]
        .paragraphs[f"{rid}/t/text/0-10"]
        .vectorsets_sentences["my-vectorset"]
        .sentences
    )
    assert len(sentences) == 1
    assert sentences[f"{rid}/t/text/0/0-10"].metadata.position.start == 0
    assert sentences[f"{rid}/t/text/0/0-10"].metadata.position.end == 10
    assert len(sentences[f"{rid}/t/text/0/0-10"].vector) == MATRYOSHKA_DIMENSION
