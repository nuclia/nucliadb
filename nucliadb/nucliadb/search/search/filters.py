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
from typing import Any, Optional, Union, cast

from nucliadb_models.filtering import FilterExpression, Terms
from nucliadb_models.labels import translate_alias_to_system_label
from nucliadb_protos import knowledgebox_pb2

from .exceptions import InvalidQueryError

ENTITY_PREFIX = "/e/"
CLASSIFICATION_LABEL_PREFIX = "/l/"


def translate_label(label: str) -> str:
    """
    Translate friendly filter label names to the shortened filter label names.
    >>> translate_label("/metadata.language/es")
    '/s/p/es'
    >>> translate_label("/entity/GPE/Sevilla")
    '/e/GPE/Sevilla'
    """
    if len(label) == 0:
        raise InvalidQueryError("filters", f"Invalid empty label")
    if label[0] != "/":
        raise InvalidQueryError(
            "filters", f"Invalid label. It must start with a `/`: {label}"
        )
    return translate_alias_to_system_label(label)


def translate_expression_labels(filters: FilterExpression) -> FilterExpression:
    """
    Translate friendly filter names to the shortened filter names.

    >>> expression = {"and": ["/metadata.language/es", "/entity/GPE/Sevilla"]}
    >>> translate_expression_labels(expression)
    {'and': ['/e/GPE/Barcelona', '/n/i/pdf']}
    """
    output: dict[str, Any] = {}
    if "and" in filters:
        and_terms: Terms = []
        for term in filters["and"]:
            if isinstance(term, str):
                term = cast(str, term)
                and_terms.append(translate_label(term))
            else:
                term = cast(FilterExpression, term)
                and_terms.append(translate_expression_labels(term))
        output["and"] = and_terms

    if "or" in filters:
        or_terms: Terms = []
        for term in filters["or"]:
            if isinstance(term, str):
                term = cast(str, term)
                or_terms.append(translate_label(term))
            else:
                term = cast(FilterExpression, term)
                or_terms.append(translate_expression_labels(term))
        output["or"] = or_terms

    if "not" in filters:
        not_term = filters["not"]
        if isinstance(not_term, str):
            not_term = cast(str, not_term)
            output["not"] = translate_label(not_term)
        elif isinstance(not_term, list):
            not_term = cast(Terms, not_term)
            not_terms: Terms = []
            for inner_term in not_term:
                if isinstance(inner_term, str):
                    not_terms.append(translate_label(inner_term))
                else:
                    not_terms.append(translate_expression_labels(inner_term))
            output["not"] = not_terms
        else:
            not_term = cast(FilterExpression, not_term)
            output["not"] = translate_expression_labels(not_term)
    return output


def iter_labels(expression: Union[str, list[str], dict[str, Any]]) -> Iterable[str]:
    """
    Iterate over all the labels in the expression.
    >>> list(iter_labels("/foo/bar"))
    ['/foo/bar']
    >>> list(iter_labels(["foo", "bar"]))
    ['foo', 'bar']
    >>> list(iter_labels({"and": ["foo", "bar"]}))
    ['foo', 'bar']
    """
    if isinstance(expression, str):
        yield expression

    elif isinstance(expression, list):
        for term in expression:
            for inner_term in iter_labels(term):
                yield inner_term

    elif isinstance(expression, dict):
        for term in expression.get("and", []):
            for inner_term in iter_labels(term):
                yield inner_term
        for term in expression.get("or", []):
            for inner_term in iter_labels(term):
                yield inner_term
        not_terms = expression.get("not")
        if not_terms is not None:
            for term in iter_labels(not_terms):
                yield term


def split_labels_by_type(
    filters: Union[list[str], dict[str, Any]],
    classification_labels: knowledgebox_pb2.Labels,
) -> tuple[list[str], list[str]]:
    """
    Split the labels into field labels and paragraph labels.
    """
    field_labels = []
    paragraph_labels = []
    for label in iter_labels(filters):
        if len(label) == 0 or label[0] != "/":
            continue
        if not label.startswith(CLASSIFICATION_LABEL_PREFIX):
            field_labels.append(label)
            continue
        # Classification labels should have the form /l/labelset/label
        parts = label.split("/")
        if len(parts) < 4:
            field_labels.append(label)
            continue
        labelset_id = parts[2]
        if is_paragraph_labelset_kind(labelset_id, classification_labels):
            paragraph_labels.append(label)
        else:
            field_labels.append(label)
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


def has_classification_label_filters(filters: Union[list[str], dict[str, Any]]) -> bool:
    return any(
        filter.startswith(CLASSIFICATION_LABEL_PREFIX)
        for filter in iter_labels(filters)
    )
