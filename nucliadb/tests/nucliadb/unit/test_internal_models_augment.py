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
from uuid import uuid4

from typing_extensions import assert_never

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.models.internal import augment
from nucliadb.search.augmentor.fields import dedup_field_select
from nucliadb.search.augmentor.paragraphs import dedup_paragraph_select
from nucliadb.search.augmentor.resources import dedup_resource_select


def test_augment():
    _ = augment.AugmentRequest.model_validate(
        {
            "augmentations": [
                {
                    "given": [uuid4().hex],
                    "select": [
                        {"prop": "title"},
                        {"prop": "summary"},
                        {"prop": "origin"},
                        {"prop": "security"},
                    ],
                    "from": "resources",
                },
                {
                    "given": [
                        FieldId.from_string(f"{uuid4().hex}/t/text"),
                        FieldId.from_string(f"{uuid4().hex}/f/file"),
                    ],
                    "select": [
                        {"prop": "value"},
                        {"prop": "text"},
                    ],
                    "from": "fields",
                },
                {
                    "given": [
                        paragraph_from_id(f"{uuid4().hex}/t/text/0-10"),
                        paragraph_from_id(f"{uuid4().hex}/f/file/20-25"),
                    ],
                    "select": [
                        {"prop": "text"},
                        {"prop": "image"},
                        {"prop": "table"},
                    ],
                    "from": "paragraphs",
                },
            ]
        }
    )


def test_full_resource_strategy():
    resource_ids = [f"{uuid4().hex}", f"{uuid4().hex}"]

    _ = augment.AugmentRequest.model_validate(
        {
            "augmentations": [
                {
                    "given": resource_ids,
                    "select": [
                        {"prop": "text"},
                    ],
                    "from": "fields",
                }
            ]
        }
    )


def test_field_extension_strategy():
    resource_ids = [f"{uuid4().hex}", f"{uuid4().hex}"]

    _ = augment.AugmentRequest.model_validate(
        {
            "augmentations": [
                {
                    "given": resource_ids,
                    "select": [
                        {"prop": "text"},
                    ],
                    "from": "fields",
                    "filter": [
                        {
                            "prop": "field",
                            "type": "generic",
                            "name": "title",
                        }
                    ],
                }
            ]
        }
    )


def test_metadata_extension_strategy():
    paragraph_ids = [
        FieldId.from_string(f"{uuid4().hex}/t/text/0-10"),
        FieldId.from_string(f"{uuid4().hex}/f/file/20-25"),
    ]

    _ = augment.AugmentRequest.model_validate(
        {
            "augmentations": [
                {
                    "given": paragraph_ids,
                    "select": [
                        {"prop": "origin"},
                        {"prop": "extra"},
                        {"prop": "classification_labels"},
                    ],
                    "from": "resources",
                },
                {
                    "given": paragraph_ids,
                    "select": [
                        {"prop": "classification_labels"},
                        {"prop": "entities"},
                    ],
                    "from": "fields",
                },
            ]
        }
    )


def test_neighbouring_paragraph_strategy():
    paragraph_ids = [
        paragraph_from_id(f"{uuid4().hex}/t/text/0-10"),
        paragraph_from_id(f"{uuid4().hex}/f/file/20-25"),
    ]

    _ = augment.AugmentRequest.model_validate(
        {
            "augmentations": [
                {
                    "given": paragraph_ids,
                    "select": [
                        {
                            "prop": "related",
                            "neighbours_before": 2,
                            "neighbours_after": 2,
                        },
                    ],
                    "from": "paragraphs",
                }
            ]
        }
    )


def test_hierarchy_strategy():
    paragraph_ids = [
        paragraph_from_id(f"{uuid4().hex}/t/text/0-10"),
        paragraph_from_id(f"{uuid4().hex}/f/file/20-25"),
    ]
    rids = [paragraph.id.rid for paragraph in paragraph_ids]

    _ = augment.AugmentRequest.model_validate(
        {
            "augmentations": [
                {
                    "given": rids,
                    "select": [
                        {"prop": "title"},
                        {"prop": "summary"},
                    ],
                    "from": "resources",
                },
                {
                    "given": paragraph_ids,
                    "select": [
                        {"prop": "text"},
                    ],
                    "from": "paragraphs",
                },
            ]
        }
    )


def test_conversational_strategy():
    field_ids = [
        FieldId.from_string(f"{uuid4().hex}/t/text"),
        FieldId.from_string(f"{uuid4().hex}/f/file"),
    ]

    _ = augment.AugmentRequest.model_validate(
        {
            "augmentations": [
                {
                    "given": field_ids,
                    "select": [
                        {"prop": "attachments", "kind": "text"},
                        {"prop": "attachments", "kind": "image"},
                    ],
                    "from": "conversations",
                    "limits": {"max_messages": 5},
                },
                {
                    "given": field_ids,
                    "select": [
                        # we do have this implemented but not exposed. Given a
                        # conversation, if it's a question try to find a
                        # following message marked as answer in the same page
                        {
                            "prop": "text",
                            "selector": {"name": "answer"},
                        },
                    ],
                    "from": "conversations",
                },
            ]
        }
    )


def paragraph_from_id(id: str) -> augment.Paragraph:
    return augment.Paragraph(
        id=ParagraphId.from_string(id),
        metadata=None,
    )


def test_paragraph_prop_deduplicator_assumptions() -> None:
    """In order to deduplicate props, augmentor assumes some have no other field
    than it's own `prop` literal. However, this might change.

    This test ensures the assumptions taken by the deduplicators are correct.

    """
    prop: augment.ParagraphProp
    paragraph_props: list[tuple[augment.ParagraphProp, set[str] | None]] = [
        (augment.ParagraphText(), None),
        (augment.ParagraphPosition(), None),
        (augment.ParagraphImage(), None),
    ]
    prop = augment.ParagraphTable()
    paragraph_props.append(
        (
            prop,
            {f"{prop.prefer_page_preview =}".split()[0].removeprefix("prop.")},
        )
    )
    prop = augment.ParagraphPage()
    paragraph_props.append(
        (
            prop,
            {f"{prop.preview =}".split()[0].removeprefix("prop.")},
        )
    )
    prop = augment.RelatedParagraphs(neighbours_before=0, neighbours_after=0)
    paragraph_props.append(
        (
            prop,
            {
                f"{prop.neighbours_before =}".split()[0].removeprefix("prop."),
                f"{prop.neighbours_after =}".split()[0].removeprefix("prop."),
            },
        )
    )

    for prop, field_names in paragraph_props:
        if (
            isinstance(prop, augment.ParagraphText)
            or isinstance(prop, augment.ParagraphPosition)
            or isinstance(prop, augment.ParagraphImage)
            or isinstance(prop, augment.ParagraphTable)
            or isinstance(prop, augment.ParagraphPage)
            or isinstance(prop, augment.RelatedParagraphs)
        ):
            if not field_names:
                assert prop.__class__.model_fields.keys() == {"prop"}
            else:
                assert prop.__class__.model_fields.keys() == {"prop"} | field_names
        else:
            assert_never(prop)

    props = [prop for prop, _ in paragraph_props]
    assert dedup_paragraph_select([*props, *props]) == props


def test_field_prop_deduplicator_assumptions() -> None:
    field_props: list[augment.FieldProp | augment.FileProp | augment.ConversationProp] = [
        augment.FieldText(),
        augment.FieldValue(),
        augment.FieldClassificationLabels(),
        augment.FieldEntities(),
        augment.FileThumbnail(),
        augment.ConversationAnswerOrAfter(),
        augment.ConversationText(selector=augment.MessageSelector()),
        augment.ConversationAttachments(selector=augment.MessageSelector()),
    ]

    for prop in field_props:
        if isinstance(prop, augment.ConversationText) or isinstance(
            prop, augment.ConversationAttachments
        ):
            assert prop.__class__.model_fields.keys() == {
                "prop",
                f"{prop.selector =}".split()[0].removeprefix("prop."),
            }

        elif (
            isinstance(prop, augment.FieldText)
            or isinstance(prop, augment.FieldValue)
            or isinstance(prop, augment.FieldClassificationLabels)
            or isinstance(prop, augment.FieldEntities)
            or isinstance(prop, augment.FileThumbnail)
            or isinstance(prop, augment.ConversationAnswerOrAfter)
        ):
            assert prop.__class__.model_fields.keys() == {"prop"}

        else:
            assert_never(prop)

    # conversation text and attachments are not deduplicated
    mergeable_props = field_props[:-2]
    assert dedup_field_select([*mergeable_props, *mergeable_props]) == mergeable_props
    unmergeable_props = field_props[-2:]
    assert dedup_field_select([*unmergeable_props, *unmergeable_props]) == [
        *unmergeable_props,
        *unmergeable_props,
    ]


def test_resource_prop_deduplicator_assumptions() -> None:
    resource_props: list[augment.ResourceProp] = [
        augment.ResourceTitle(),
        augment.ResourceSummary(),
        augment.ResourceOrigin(),
        augment.ResourceExtra(),
        augment.ResourceSecurity(),
        augment.ResourceClassificationLabels(),
    ]

    for prop in resource_props:
        if (
            isinstance(prop, augment.ResourceTitle)
            or isinstance(prop, augment.ResourceSummary)
            or isinstance(prop, augment.ResourceOrigin)
            or isinstance(prop, augment.ResourceExtra)
            or isinstance(prop, augment.ResourceSecurity)
            or isinstance(prop, augment.ResourceClassificationLabels)
        ):
            assert prop.__class__.model_fields.keys() == {"prop"}

        else:
            assert_never(prop)

    assert dedup_resource_select([*resource_props, *resource_props]) == resource_props
