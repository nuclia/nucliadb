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

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.models.internal import augment


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
                        # TODO(decoupled-ask): props for classification_labels, ner...
                    ],
                    "from": "resources",
                }
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
