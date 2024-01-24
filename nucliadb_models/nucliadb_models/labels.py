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

from pydantic import BaseModel

BASE_LABELS: dict[str, list[str]] = {
    "t": [],  # doc tags
    "l": [],  # doc labels
    "n": [],  # type of element: i (Icon). s (Processing Status)
    "e": [],  # entities e/type/entityid
    "s": [],  # languages p (Principal) s (ALL)
    "u": [],  # contributors s (Source) o (Origin)
    "f": [],  # field keyword field (field/keyword)
    "fg": [],  # field keyword (keywords) flat
    "m": [],  # origin metadata in the form of (key/value). Max key/value size is 255
    "p": [],  # origin metadata in the form of (key/value). Max key/value size is 255
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
    "classification.labels": "l",
    "entities": "e",
    "field": "f",
    "field-values": "fg",
}

LABEL_QUERY_ALIASES_REVERSED = {v: k for k, v in LABEL_QUERY_ALIASES.items()}


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


def flatten_resource_labels(tags_dict: dict[str, list[str]]) -> list[str]:
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
    title: Optional[str] = "no title"
    color: Optional[str] = "blue"
    multiple: bool = True
    kind: List[LabelSetKind] = []
    labels: List[Label] = []


class KnowledgeBoxLabels(BaseModel):
    uuid: str
    labelsets: Dict[str, LabelSet] = {}
