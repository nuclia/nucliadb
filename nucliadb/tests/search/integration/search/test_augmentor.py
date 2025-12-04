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

from typing import AsyncIterator

import pytest
from httpx import AsyncClient

from nucliadb.common.ids import ParagraphId
from nucliadb.models.internal.augment import (
    Paragraph,
    ParagraphAugment,
    ParagraphText,
    RelatedParagraphs,
    ResourceAugment,
    ResourceOrigin,
    ResourceSummary,
    ResourceTitle,
)
from nucliadb.search.augmentor.augmentor import augment
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import cookie_tale_resource, smb_wonder_resource


@pytest.fixture
async def augmentor_kb(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> AsyncIterator[tuple[str, dict[str, str]]]:
    kbid = knowledgebox
    rids = {
        "smb-wonder": await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc),
        "cookie-tale": await cookie_tale_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc),
    }
    yield kbid, rids


@pytest.mark.deploy_modes("standalone")
async def test_augmentor_metadata_extension_strategy(
    augmentor_kb: tuple[str, dict[str, str]],
) -> None:
    kbid, rids = augmentor_kb

    augmented = await augment(
        kbid,
        [
            ResourceAugment(
                given=[rids["smb-wonder"], rids["cookie-tale"]],
                select=[ResourceOrigin()],
            ),
        ],
    )
    resource = augmented.resources[rids["smb-wonder"]]
    assert resource.origin is not None
    assert resource.origin.source_id == "My Source"
    assert resource.origin.url == ""

    resource = augmented.resources[rids["cookie-tale"]]
    assert resource.origin is not None
    assert resource.origin.source_id == "My Source"
    assert resource.origin.url == "my://url"


@pytest.mark.deploy_modes("standalone")
async def test_augmentor_neighbouring_paragraphs_strategy(
    augmentor_kb: tuple[str, dict[str, str]],
) -> None:
    kbid, rids = augmentor_kb
    rid = rids["smb-wonder"]

    augmented = await augment(
        kbid,
        [
            ParagraphAugment(
                given=[paragraph_from_id(f"{rid}/f/smb-wonder/99-145")],
                select=[RelatedParagraphs(neighbours_before=0, neighbours_after=0)],
            ),
        ],
    )
    assert len(augmented.paragraphs) == 1
    (_, paragraph) = augmented.paragraphs.popitem()
    assert paragraph.related is not None
    assert len(paragraph.related.neighbours_before) == 0
    assert len(paragraph.related.neighbours_after) == 0

    augmented = await augment(
        kbid,
        [
            ParagraphAugment(
                given=[paragraph_from_id(f"{rid}/f/smb-wonder/99-145")],
                select=[RelatedParagraphs(neighbours_before=10, neighbours_after=10)],
            ),
        ],
    )
    assert len(augmented.paragraphs) == 1
    (_, paragraph) = augmented.paragraphs.popitem()
    assert paragraph.related is not None
    assert paragraph.related.neighbours_before == [ParagraphId.from_string(f"{rid}/f/smb-wonder/0-99")]
    assert paragraph.related.neighbours_after == [ParagraphId.from_string(f"{rid}/f/smb-wonder/145-234")]

    augmented = await augment(
        kbid,
        [
            ParagraphAugment(
                given=[paragraph_from_id(f"{rid}/f/smb-wonder/0-99")],
                select=[RelatedParagraphs(neighbours_before=10, neighbours_after=10)],
            ),
        ],
    )
    assert len(augmented.paragraphs) == 1
    (_, paragraph) = augmented.paragraphs.popitem()
    assert paragraph.related is not None
    assert len(paragraph.related.neighbours_before) == 0
    assert paragraph.related.neighbours_after == [
        ParagraphId.from_string(f"{rid}/f/smb-wonder/99-145"),
        ParagraphId.from_string(f"{rid}/f/smb-wonder/145-234"),
    ]

    augmented = await augment(
        kbid,
        [
            ParagraphAugment(
                given=[paragraph_from_id(f"{rid}/f/smb-wonder/145-234")],
                select=[RelatedParagraphs(neighbours_before=10, neighbours_after=10)],
            ),
        ],
    )
    assert len(augmented.paragraphs) == 1
    (_, paragraph) = augmented.paragraphs.popitem()
    assert paragraph.related is not None
    assert paragraph.related.neighbours_before == [
        ParagraphId.from_string(f"{rid}/f/smb-wonder/0-99"),
        ParagraphId.from_string(f"{rid}/f/smb-wonder/99-145"),
    ]
    assert len(paragraph.related.neighbours_after) == 0


@pytest.mark.deploy_modes("standalone")
async def test_augmentor_hierarchy_strategy(
    augmentor_kb: tuple[str, dict[str, str]],
) -> None:
    kbid, rids = augmentor_kb
    rid = rids["smb-wonder"]

    augmented = await augment(
        kbid,
        [
            ResourceAugment(
                given=[rid],
                select=[ResourceTitle(), ResourceSummary()],
            ),
            ParagraphAugment(
                given=[paragraph_from_id(f"{rid}/f/smb-wonder/99-145")],
                select=[ParagraphText()],
            ),
        ],
    )

    resource = augmented.resources[rid]
    assert resource.title == "Super Mario Bros. Wonder"
    assert resource.summary == "SMB Wonder: the new Mario game from Nintendo"
    (_, paragraph) = augmented.paragraphs.popitem()
    assert paragraph.text == "SMB Wonder is a side-scrolling plaftorm game.\n"


def paragraph_from_id(id: str) -> Paragraph:
    return Paragraph(
        id=ParagraphId.from_string(id),
        metadata=None,
    )
