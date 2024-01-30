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
from collections.abc import Iterable
from typing import Optional

from numpy import isin

from nucliadb_models.filtering import AND, FilterExpression
from nucliadb_models.labels import translate_alias_to_system_label
from nucliadb_protos import knowledgebox_pb2

from .exceptions import InvalidQueryError

ENTITY_PREFIX = "/e/"
CLASSIFICATION_LABEL_PREFIX = "/l/"


def translate_filter(fltr: str) -> str:
    """
    Translate friendly filter names to the shortened filter names.
    """
    if len(fltr) == 0:
        raise InvalidQueryError("filters", f"Invalid empty label")
    if fltr[0] != "/":
        raise InvalidQueryError(
            "filters", f"Invalid label. It must start with a `/`: {fltr}"
        )
    return translate_alias_to_system_label(fltr)


def translate_expression_labels(filters: FilterExpression) -> FilterExpression:
    """
    Translate friendly filter names to the shortened filter names.

    >>> expression = {"and": ["/metadata.language/es", "/entity/GPE/Sevilla"]}
    >>> translate_expression_labels(expression)
    {'and': ['/e/GPE/Barcelona', '/n/i/pdf']}
    """
    output = {}
    if "and" in filters:
        terms = []
        for term in filters["and"]:
            if isinstance(term, str):
                terms.append(translate_filter(term))
            else:
                terms.append(translate_expression_labels(term))
        output["and"] = terms

    if "or" in filters:
        terms = []
        for term in filters["or"]:
            if isinstance(term, str):
                terms.append(translate_filter(term))
            else:
                terms.append(translate_expression_labels(term))
        output["or"] = terms

    if "not" in filters:
        term = filters["not"]
        if isinstance(term, str):
            output["not"] = translate_filter(term)
        else:
            output["not"] = translate_expression_labels(term)
    return output


def iter_labels(expression: FilterExpression) -> Iterable[str]:
    """
    Iterate over all the labels in the expression.
    >>> list(iter_labels({"and": ["foo", "bar"]}))
    ['foo', 'bar']
    """
    for term in expression.get("and", []):
        if isinstance(term, str):
            yield term
        else:
            for label in iter_labels(term):
                yield label
    for term in expression.get("or", []):
        if isinstance(term, str):
            yield term
        else:
            for label in iter_labels(term):
                yield label
    not_terms = expression.get("not")
    if not_terms is not None:
        if isinstance(not_terms, str):
            yield not_terms
        else:
            for label in iter_labels(not_terms):
                yield label


def split_labels_by_type(
    filters: FilterExpression, classification_labels: knowledgebox_pb2.Labels
) -> tuple[list[str], list[str]]:
    """
    Split the labels into field labels and paragraph labels.
    """
    field_labels = []
    paragraph_labels = []
    for fltr in iter_labels(filters):
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


def has_classification_label_filters(expression: FilterExpression) -> bool:
    return any(
        label.startswith(CLASSIFICATION_LABEL_PREFIX)
        for label in iter_labels(expression)
    )


def translate_to_node_internal_filter(expression: FilterExpression) -> FilterExpression:
    """
    Flattens the expression and translates the labels to the node internal format.
    >>> expression = {"and": ["/metadata.language/es", "/entity/GPE/Sevilla"], "not": "/entity/GPE/Barcelona"}
    >>> translate_to_node_internal_filter(expression)
    {'and': [{"and": ['/e/GPE/Barcelona', '/n/i/pdf']}, '/e/GPE/Sevilla']}
    """
    global_terms = []
    if "and" in expression:
        and_terms = []
        for term in expression["and"]:
            if isinstance(term, str):
                and_terms.append(term)
            else:
                and_terms.append(translate_to_node_internal_filter(term))
        global_terms.append(and_terms)

    if "or" in expression:
        or_terms = []
        for term in expression["or"]:
            if isinstance(term, str):
                or_terms.append(term)
            else:
                or_terms.append(translate_to_node_internal_filter(term))
        global_terms.append(or_terms)

    if "not" in expression:
        not_terms = []
        if isinstance(term, str):
            not_terms.append(term)
        elif isinstance(term, list):
            for term in expression["not"]:
                not_terms.append(translate_to_node_internal_filter(term))
        else:
            not_terms.append(translate_to_node_internal_filter(term))
        global_terms.append(not_terms)

    return AND(global_terms)
