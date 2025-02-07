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

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, model_validator
from typing_extensions import Self

BASE_LABELS: dict[str, set[str]] = {
    "t": set(),  # doc tags
    "l": set(),  # doc labels
    "n": set(),  # type of element: i (Icon). s (Processing Status)
    "e": set(),  # entities e/type/entityid
    "s": set(),  # languages p (Principal) s (ALL)
    "u": set(),  # contributors s (Source) o (Origin)
    "f": set(),  # field keyword field (field/keyword)
    "fg": set(),  # field keyword (keywords) flat
    "m": set(),  # origin metadata in the form of (key/value). Max key/value size is 255
    "p": set(),  # origin metadata in the form of (key/value). Max key/value size is 255
    "k": set(),  # kind of text paragraph to be stored
    "q": set(),  # reserved for internal use: h (hidden)
    "mt": set(),  # field mime type
    "g": set(),  # field generated by (e.g. data augmentation)
}


LABEL_QUERY_ALIASES = {
    # aliases to make querying labels easier
    "icon": "n/i",
    "metadata.status": "n/s",
    "metadata.language": "s/p",
    "metadata.languages": "s/s",
    "origin.tags": "t",
    "origin.metadata": "m",
    "origin.path": "p",
    "origin.source-id": "u/s",
    "classification.labels": "l",
    "entities": "e",
    "field": "f",
    "field-values": "fg",
    "generated.data-augmentation": "g/da",
}

LABEL_QUERY_ALIASES_REVERSED = {v: k for k, v in LABEL_QUERY_ALIASES.items()}

LABEL_HIDDEN = "/q/h"


def translate_alias_to_system_label(label: str) -> str:
    parts = label.split("/")
    if parts[1] in LABEL_QUERY_ALIASES:
        parts = [""] + [LABEL_QUERY_ALIASES[parts[1]]] + parts[2:]
        return "/".join(parts)
    else:
        return label


def translate_system_to_alias_label(label: str) -> str:
    parts = label.split("/")
    if parts[1] in LABEL_QUERY_ALIASES_REVERSED:
        parts = [""] + [LABEL_QUERY_ALIASES_REVERSED[parts[1]]] + parts[2:]
        return "/".join(parts)
    elif "/".join(parts[1:3]) in LABEL_QUERY_ALIASES_REVERSED:
        parts = [""] + [LABEL_QUERY_ALIASES_REVERSED["/".join(parts[1:3])]] + parts[3:]
        return "/".join(parts)
    else:
        return label


def flatten_resource_labels(tags_dict: dict[str, set[str]]) -> list[str]:
    flat_tags = []
    for key, values in tags_dict.items():
        for value in values:
            flat_tags.append(f"/{key}/{value}")
    return flat_tags


class LabelSetKind(str, Enum):
    RESOURCES = "RESOURCES"
    PARAGRAPHS = "PARAGRAPHS"
    SENTENCES = "SENTENCES"
    SELECTIONS = "SELECTIONS"


class Label(BaseModel):
    title: str
    related: Optional[str] = None
    text: Optional[str] = None
    uri: Optional[str] = None


class LabelSet(BaseModel):
    title: Optional[str] = None
    color: Optional[str] = "blue"
    multiple: bool = True
    kind: List[LabelSetKind] = []
    labels: List[Label] = []

    @model_validator(mode="after")
    def check_unique_labels(self) -> Self:
        label_ids = set()
        for label in self.labels:
            label_id = label.title.lower()
            if label_id in label_ids:
                raise ValueError(f"Duplicated labels are not allowed: {label.title}")
            label_ids.add(label_id)

        return self


class KnowledgeBoxLabels(BaseModel):
    uuid: str
    labelsets: Dict[str, LabelSet] = {}
