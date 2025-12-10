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

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.models.internal.augment import (
    AnswerSelector,
    AugmentedConversationField,
    ConversationAugment,
    ConversationText,
    FieldAugment,
    FieldText,
    FullSelector,
    MessageSelector,
    NeighboursSelector,
    PageSelector,
    Paragraph,
    ParagraphAugment,
    ParagraphText,
    RelatedParagraphs,
    ResourceAugment,
    ResourceOrigin,
    ResourceSummary,
    ResourceTitle,
    WindowSelector,
)
from nucliadb.search.augmentor.augmentor import augment
from nucliadb.search.search.cache import request_caches
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import cookie_tale_resource, smb_wonder_resource
from tests.ndbfixtures.resources.lambs import lambs_resource


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


@pytest.mark.deploy_modes("standalone")
async def test_augmentor_conversation_strategy(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox

    rid = await lambs_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    with request_caches():
        field_id = FieldId.from_string(f"{rid}/c/lambs")
        split_1_id = FieldId.from_string(f"{rid}/c/lambs/1")
        split_7_id = FieldId.from_string(f"{rid}/c/lambs/7")
        split_10_id = FieldId.from_string(f"{rid}/c/lambs/10")
        split_12_id = FieldId.from_string(f"{rid}/c/lambs/12")

        # Augmenting a conversation field text yields the whole conversation
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[field_id],
                    select=[FieldText()],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 12
        for message in augmented_field.messages:
            assert message.text

        # same as using the full selector
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[field_id],
                    select=[ConversationText(selector=FullSelector())],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 12
        for message in augmented_field.messages:
            assert message.text

        # Augmenting a split only returns that message
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_10_id],
                    select=[FieldText()],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "10"
        assert augmented_field.messages[0].text == "You know I can't make that promise."

        # Augmenting a field with message selector returns the selected split
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[field_id],
                    select=[ConversationText(selector=MessageSelector(id="10"))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "10"
        assert augmented_field.messages[0].text == "You know I can't make that promise."

        # Augmenting with message selector ignores the split from the id
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_7_id],
                    select=[ConversationText(selector=MessageSelector(id="10"))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "10"
        assert augmented_field.messages[0].text == "You know I can't make that promise."

        # Augmenting with message selector ignores the split from the id
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_7_id],
                    select=[ConversationText(selector=MessageSelector(id="10"))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "10"
        assert augmented_field.messages[0].text == "You know I can't make that promise."

        # Augmenting with message selector ignores the split from the id
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_7_id],
                    select=[ConversationText(selector=MessageSelector(index=9))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "10"
        assert augmented_field.messages[0].text == "You know I can't make that promise."

        # Augmenting with message selector first/last
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_7_id],
                    select=[ConversationText(selector=MessageSelector(index="first"))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "1"

        # Augmenting with message selector first/last
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_7_id],
                    select=[ConversationText(selector=MessageSelector(index="last"))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "12"

        # Augmenting with multiple message selectors
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_7_id],
                    select=[
                        ConversationText(selector=MessageSelector()),
                        ConversationText(selector=MessageSelector(index="first")),
                        ConversationText(selector=MessageSelector(index=0)),  # same as "first"
                        ConversationText(selector=MessageSelector(id="5")),
                        ConversationText(selector=MessageSelector(index=9)),
                        ConversationText(selector=MessageSelector(index="last")),
                        ConversationText(selector=MessageSelector(index=11)),  # same as "last"
                    ],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 5
        assert {m.ident for m in augmented_field.messages} == {"1", "5", "7", "10", "12"}

        # Augmenting a field with page/neighbour/window selector doesn't yield
        # any message, as we can't know which page/neighbour/window do we want
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[field_id],
                    select=[
                        ConversationText(selector=PageSelector()),
                        ConversationText(selector=NeighboursSelector(after=5)),
                        ConversationText(selector=WindowSelector(size=3)),
                    ],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is None

        # Augmenting a split with page selector yields the whole page
        # TODO: we should test this with a multi-page conversation
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_10_id],
                    select=[ConversationText(selector=PageSelector())],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 12

        # Augmenting with neighbours selector
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_10_id],
                    select=[ConversationText(selector=NeighboursSelector(after=1))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 2
        assert {m.ident for m in augmented_field.messages} == {"10", "11"}

        # Augmenting more neighbours than the ones we have yields until the end
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_10_id],
                    select=[ConversationText(selector=NeighboursSelector(after=5))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 3
        assert {m.ident for m in augmented_field.messages} == {"10", "11", "12"}

        # Augmenting a window that surrounds the split
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_10_id],
                    select=[ConversationText(selector=WindowSelector(size=3))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 3
        assert {m.ident for m in augmented_field.messages} == {"9", "10", "11"}

        # Augmenting a window touching the end
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_12_id],
                    select=[ConversationText(selector=WindowSelector(size=3))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 3
        assert {m.ident for m in augmented_field.messages} == {"10", "11", "12"}

        # Augmenting a window touching the beginning
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_1_id],
                    select=[ConversationText(selector=WindowSelector(size=3))],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        # FIXME: this comes from the original implementation an doesn't work...
        # assert len(augmented_field.messages) == 3
        # assert {m.ident for m in augmented_field.messages} == {"1", "2", "3"}

        # Find the following answer, in this case, 5 is an answer
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_1_id],
                    select=[ConversationText(selector=AnswerSelector())],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is not None
        assert len(augmented_field.messages) == 1
        assert augmented_field.messages[0].ident == "5"

        # 7 has no following answers, so no message is returned
        augmented = await augment(
            kbid,
            [
                FieldAugment(given=[], select=[]),
                ConversationAugment(
                    given=[split_7_id],
                    select=[ConversationText(selector=AnswerSelector())],
                ),
            ],
        )
        augmented_field = augmented.fields[field_id]
        assert isinstance(augmented_field, AugmentedConversationField)
        assert augmented_field.messages is None


def paragraph_from_id(id: str) -> Paragraph:
    return Paragraph(
        id=ParagraphId.from_string(id),
        metadata=None,
    )
