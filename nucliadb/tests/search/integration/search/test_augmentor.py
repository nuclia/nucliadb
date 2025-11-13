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
    ParagraphAugment,
    ParagraphText,
    ResourceAugment,
    ResourceOrigin,
    ResourceSecurity,
    ResourceSummary,
    ResourceTitle,
)
from nucliadb.search.augmentor.augmentor import augment
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import smb_wonder_resource


@pytest.fixture
async def augmentor_kb(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> AsyncIterator[tuple[str, dict[str, str]]]:
    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    yield kbid, {"smb-wonder": rid}


@pytest.mark.skip  # TODO: remove skip, only to iterate tests
@pytest.mark.deploy_modes("standalone")
async def test_augmentor(
    augmentor_kb: tuple[str, dict[str, str]],
) -> None:
    kbid, rids = augmentor_kb
    rid = rids["smb-wonder"]

    # Simple augmentation example

    augmented = await augment(
        kbid,
        [
            ResourceAugment(
                given=[rid],
                select=[ResourceTitle()],
            ),
            ResourceAugment(
                given=[rid],
                select=[ResourceSummary()],
            ),
            ResourceAugment(
                given=[rid],
                select=[ResourceOrigin(), ResourceSecurity()],
            ),
        ],
    )

    resource = augmented.resources[rid]
    assert resource.title is not None
    assert resource.title == "Super Mario Bros. Wonder"
    assert resource.summary is not None
    assert resource.summary == "SMB Wonder: the new Mario game from Nintendo"
    assert resource.origin is not None
    assert resource.origin.source_id == "My Source"
    assert resource.security is not None
    assert resource.security.access_groups == []


@pytest.mark.skip  # TODO: remove skip, only to iterate tests
@pytest.mark.deploy_modes("standalone")
async def test_augmentor_metadata_extension_strategy(
    augmentor_kb: tuple[str, dict[str, str]],
) -> None:
    kbid, rids = augmentor_kb
    rid = rids["smb-wonder"]

    augmented = await augment(
        kbid,
        [
            ResourceAugment(
                given=[rid],
                select=[ResourceOrigin()],
            ),
        ],
    )
    resource = augmented.resources[rid]
    assert resource.origin is not None
    assert resource.origin.source_id == "My Source"


@pytest.mark.deploy_modes("standalone")
async def test_augmentor_neighbouring_paragraphs_strategy(
    augmentor_kb: tuple[str, dict[str, str]],
) -> None:
    kbid, rids = augmentor_kb
    rid = rids["smb-wonder"]
    paragraph_id = ParagraphId.from_string(f"{rid}/f/smb-wonder/99-145")

    augmented = await augment(
        kbid,
        [
            ParagraphAugment(
                given=[paragraph_id.full()],
                select=[ParagraphText()],
            ),
        ],
    )
    paragraph = augmented.paragraphs[paragraph_id]
    assert paragraph.text == "SMB Wonder is a side-scrolling plaftorm game.\n"

    # TODO: WIP
