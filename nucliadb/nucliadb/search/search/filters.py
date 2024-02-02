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
from typing import Optional

from nucliadb_models.labels import translate_alias_to_system_label
from nucliadb_models.search import Filter
from nucliadb_protos import knowledgebox_pb2
from nucliadb_telemetry.metrics import Counter

from .exceptions import InvalidQueryError

ENTITY_PREFIX = "/e/"
CLASSIFICATION_LABEL_PREFIX = "/l/"


def translate_label_filters(filters: list[str]) -> list[str]:
    """
    Translate friendly filter names to the shortened filter names.
    """
    output = []
    for fltr in filters:
        if len(fltr) == 0:
            raise InvalidQueryError("filters", f"Invalid empty label")
        if fltr[0] != "/":
            raise InvalidQueryError(
                "filters", f"Invalid label. It must start with a `/`: {fltr}"
            )

        output.append(translate_alias_to_system_label(fltr))
    return output


def record_filters_counter(filters: list[str], counter: Counter) -> None:
    counter.inc({"type": "filters"})
    filters.sort()
    entity_found = False
    label_found = False
    for fltr in filters:
        if entity_found and label_found:
            break
        if not entity_found and fltr.startswith(ENTITY_PREFIX):
            entity_found = True
            counter.inc({"type": "filters_entities"})
        elif not label_found and fltr.startswith(CLASSIFICATION_LABEL_PREFIX):
            label_found = True
            counter.inc({"type": "filters_labels"})


def split_labels_by_type(
    filters: list[str], classification_labels: knowledgebox_pb2.Labels
) -> tuple[list[str], list[str]]:
    field_labels = []
    paragraph_labels = []
    for fltr in filters:
        if len(fltr) == 0 or fltr[0] != "/":
            continue
        if not fltr.startswith(CLASSIFICATION_LABEL_PREFIX):
            field_labels.append(fltr)
            continue
        # Classification labels should have the form /l/labelset/label
        parts = fltr.split("/")
        if len(parts) < 4:
            field_labels.append(fltr)
            continue
        labelset_id = parts[2]
        if is_paragraph_labelset_kind(labelset_id, classification_labels):
            paragraph_labels.append(fltr)
        else:
            field_labels.append(fltr)
    return field_labels, paragraph_labels


def is_paragraph_labelset_kind(
    labelset_id: str, classification_labels: knowledgebox_pb2.Labels
) -> bool:
    try:
        labelset: Optional[
            knowledgebox_pb2.LabelSet
        ] = classification_labels.labelset.get(labelset_id)
        if labelset is None:
            return False
        return knowledgebox_pb2.LabelSet.LabelSetKind.PARAGRAPHS in labelset.kind
    except KeyError:
        # labelset_id not found
        return False


def has_classification_label_filters(filters: list[str]) -> bool:
    return any(filter.startswith(CLASSIFICATION_LABEL_PREFIX) for filter in filters)


def convert_to_node_filters(filters: list[Filter]) -> dict:
    if len(filters) == 1:
        return convert_filter_to_node_schema(filters[0])

    return {"and": [convert_filter_to_node_schema(fltr) for fltr in filters]}


def convert_filter_to_node_schema(fltr: Filter) -> dict:
    # any: [a, b] == (a || b)
    if fltr.any is not None:
        if len(fltr.any) == 1:
            return {"literal": fltr.any[0]}
        return {"or": [{"literal": term} for term in fltr.any]}

    # all: [a, b] == (a && b)
    if fltr.all is not None:
        if len(fltr.all) == 1:
            return {"literal": fltr.all[0]}
        return {"and": [{"literal": term} for term in fltr.all]}

    # none: [a, b] == !(a || b)
    if fltr.none is not None:
        if len(fltr.none) == 1:
            return {"not": {"literal": fltr.none[0]}}
        return {"not": {"or": [{"literal": term} for term in fltr.none]}}

    # not_all: [a, b] == !(a && b)
    if fltr.not_all is not None:
        if len(fltr.not_all) == 1:
            return {"not": {"literal": fltr.not_all[0]}}
        return {"not": {"and": [{"literal": term} for term in fltr.not_all]}}

    raise ValueError("Invalid filter")


NODE_FILTERS_SCHEMA = {
    "definitions": {
        "Filter": {
            "type": "object",
            "minProperties": 1,
            "additionalProperties": False,
            "properties": {
                "literal": {"type": "string"},
                "and": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"$ref": "#/definitions/Filter"},
                },
                "or": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"$ref": "#/definitions/Filter"},
                },
                "not": {"$ref": "#/definitions/Filter"},
            },
        }
    }
}
