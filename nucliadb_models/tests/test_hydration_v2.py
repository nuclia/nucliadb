# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from uuid import uuid4

from nucliadb_models import hydration_v2


def test_hydration_v2():
    _ = hydration_v2.HydrationRequest.model_validate(
        {
            "hydrations": [
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
                    "given": [f"{uuid4().hex}/t/text", f"{uuid4().hex}/f/file"],
                    "select": [
                        {"prop": "value"},
                        {"prop": "text"},
                    ],
                    "from": "fields",
                },
                {
                    "given": [f"{uuid4().hex}/t/text/0-10", f"{uuid4().hex}/f/file/20-25"],
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
    paragraph_ids = [f"{uuid4().hex}/t/text/0-10", f"{uuid4().hex}/f/file/20-25"]

    _ = hydration_v2.HydrationRequest.model_validate(
        {
            "hydrations": [
                {
                    "given": paragraph_ids,
                    "select": [
                        {
                            "prop": "fields",
                            "select": [
                                {"prop": "text"},
                            ],
                        },
                    ],
                    "from": "resources",
                }
            ]
        }
    )


def test_field_extension_strategy():
    paragraph_ids = [f"{uuid4().hex}/t/text/0-10", f"{uuid4().hex}/f/file/20-25"]

    _ = hydration_v2.HydrationRequest.model_validate(
        {
            "hydrations": [
                {
                    "given": paragraph_ids,
                    "select": [
                        {
                            "prop": "fields",
                            "select": [
                                {"prop": "text"},
                            ],
                            "filter": {"ids": ["a/title"]},
                        },
                    ],
                    "from": "resources",
                }
            ]
        }
    )


def test_metadata_extension_strategy():
    paragraph_ids = [f"{uuid4().hex}/t/text/0-10", f"{uuid4().hex}/f/file/20-25"]

    _ = hydration_v2.HydrationRequest.model_validate(
        {
            "hydrations": [
                {
                    "given": paragraph_ids,
                    "select": [
                        {"prop": "origin"},
                        # TODO: props for classification_labels, ner...
                    ],
                    "from": "resources",
                }
            ]
        }
    )


def test_neighbouring_paragraph_strategy():
    paragraph_ids = [f"{uuid4().hex}/t/text/0-10", f"{uuid4().hex}/f/file/20-25"]

    _ = hydration_v2.HydrationRequest.model_validate(
        {
            "hydrations": [
                {
                    "given": paragraph_ids,
                    "select": [
                        {
                            "prop": "related",
                            "neighbours": {
                                "before": 2,
                                "after": 2,
                            },
                        },
                    ],
                    "from": "paragraphs",
                }
            ]
        }
    )


def test_hierarchy_strategy():
    paragraph_ids = [f"{uuid4().hex}/t/text/0-10", f"{uuid4().hex}/f/file/20-25"]

    _ = hydration_v2.HydrationRequest.model_validate(
        {
            "hydrations": [
                {
                    "given": paragraph_ids,
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
    paragraph_ids = [f"{uuid4().hex}/t/text/0-10", f"{uuid4().hex}/f/file/20-25"]

    _ = hydration_v2.HydrationRequest.model_validate(
        {
            "hydrations": [
                {
                    "given": paragraph_ids,
                    "select": [
                        {"prop": "attachments", "text": True, "image": False},
                    ],
                    "from": "conversations",
                    "limits": {"max_messages": 5},
                }
            ]
        }
    )
