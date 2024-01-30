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
from typing import Any, Dict, List, Union

import jsonschema

EXPRESSION_JSON_SCHEMA = {
    "$ref": "#/definitions/FilterExpression",
    "definitions": {
        "FilterExpression": {
            "title": "FilterExpression",
            "type": "object",
            "examples": [
                {"and": ["/metadata.language/es", "/entity/GPE/Sevilla"]},
                {"and": ["/metadata.language/es", {"not": "/entity/GPE/Sevilla"}]},
                {"or": ["/entity/GPE/Barcelona", "/entity/GPE/Madrid"]},
                {"not": "/icon/application/pdf"},
                {"not": {"and": ["/entity/GPE/Sevilla", "/entity/GPE/Madrid"]}},
            ],
            "additionalProperties": False,
            "minProperties": 1,
            "properties": {
                "and": {
                    "title": "And",
                    "type": "array",
                    "minItems": 2,
                    "items": {
                        "anyOf": [
                            {"type": "string"},
                            {"$ref": "#/definitions/FilterExpression"},
                        ],
                    },
                },
                "or": {
                    "title": "Or",
                    "type": "array",
                    "minItems": 2,
                    "items": {
                        "anyOf": [
                            {"type": "string"},
                            {"$ref": "#/definitions/FilterExpression"},
                        ]
                    },
                },
                "not": {
                    "title": "Not",
                    "anyOf": [
                        {"type": "string"},
                        {"$ref": "#/definitions/FilterExpression"},
                        {
                            "type": "array",
                            "minItems": 2,
                            "items": {
                                "anyOf": [
                                    {"type": "string"},
                                    {"$ref": "#/definitions/FilterExpression"},
                                ],
                            },
                        },
                    ],
                },
            },
        }
    },
}


Value = str
FilterExpression = Dict[str, Any]
Term = Union[Value, FilterExpression]
Terms = List[Term]
TermsV1 = List[Value]


def AND(*params) -> Term:
    """
    >>> AND("foo", "bar")
    {'and': ['foo', 'bar']}
    >>> AND(["foo", "bar"])
    {'and': ['foo', 'bar']}
    >>> AND("foo")
    'foo'
    """
    if len(params) == 0:
        raise ValueError("AND() requires at least one parameter")
    if len(params) == 1:
        # AND(["foo", "bar"])
        terms = params[0]
        if isinstance(terms, list):
            return {"and": terms}
        else:
            # AND("foo")
            return terms
    else:
        # AND("foo", "bar")
        return {"and": list(params)}


def OR(*params) -> Term:
    """
    >>> OR("foo", "bar")
    {'or': ['foo', 'bar']}
    >>> OR(["foo", "bar"])
    {'or': ['foo', 'bar']}
    >>> OR("foo")
    'foo'
    """
    if len(params) == 0:
        raise ValueError("OR() requires at least one parameter")

    if len(params) == 1:
        # OR(["foo", "bar"])
        terms = params[0]
        if isinstance(terms, list):
            return {"or": terms}
        else:
            # OR("foo")
            return terms
    else:
        # OR("foo", "bar")
        return {"or": list(params)}


def NOT(*params) -> Term:
    if len(params) == 0:
        raise ValueError("NOT() requires at least one parameter")
    if len(params) == 1:
        term = params[0]
        if isinstance(term, list):
            if len(term) == 1:
                return {"not": term[0]}
            else:
                return {"not": term}
        else:
            return {"not": term}
    else:
        return {"not": list(params)}


def convert_to_v2(terms_v1: TermsV1) -> Term:
    """
    Convert old filters format to an expression
    >>> convert_to_expression(["foo", "bar"])
    {'and': ['foo', 'bar']}
    """
    return AND(terms_v1)


def validate(expression: FilterExpression):
    try:
        jsonschema.validate(expression, EXPRESSION_JSON_SCHEMA)
    except jsonschema.ValidationError as e:
        raise ValueError(f"Invalid expression in 'filters': {e}") from e
