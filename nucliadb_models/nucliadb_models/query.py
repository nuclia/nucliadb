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

"""
Proposal: Generic filtering syntax for queries

Requirements:
- Old filters should be supported: simply an AND of the input filter terms

Draft API: (see below)

Rollout plan:
- Add support for advanced filtering in the IndexNode
- Make current filters work with IndexNode's advanced filtering
- Extend the HTTP API to support advanced filtering

Doubts:
- Can we make it work with paragraph-level classifiction label filters? :(

"""
import functools
from typing import Any, Dict, List, Optional, Union

import jsonschema
from pydantic import BaseModel, Field, validator

EXPRESSION_JSON_SCHEMA = {
    "$ref": "#/definitions/Expression",
    "definitions": {
        "Expression": {
            "title": "Expression",
            "type": "object",
            "examples": [
                {"and": ["/metadata.language/es", "/entity/GPE/Sevilla"]},
                {"and": ["/metadata.language/es", {"not": "/entity/GPE/Sevilla"}]},
                {"or": ["/entity/GPE/Barcelona", "/entity/GPE/Madrid"]},
                {"not": "/icon/application/pdf"},
                {"not": {"and": ["/entity/GPE/Sevilla", "/entity/GPE/Madrid"]}},
            ],
            "additionalProperties": False,
            "maxProperties": 1,
            "minProperties": 1,
            "properties": {
                "and": {
                    "title": "And",
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {"type": "string"},
                            {"$ref": "#/definitions/Expression"},
                        ],
                        "minItems": 2,
                    },
                },
                "or": {
                    "title": "Or",
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {"type": "string"},
                            {"$ref": "#/definitions/Expression"},
                        ]
                    },
                    "minItems": 2,
                },
                "not": {
                    "title": "Not",
                    "anyOf": [{"type": "string"}, {"$ref": "#/definitions/Expression"}],
                },
            },
        }
    },
}


## Helper functions that make it easier to create expressions

Value = str
Expression = Dict[str, Any]
Term = Union[Value, Expression]
Terms = List[Term]
TermsV1 = List[Value]


def AND(*terms) -> Expression:
    """
    >>> AND("foo", "bar")
    {'and': ['foo', 'bar']}
    >>> AND(["foo", "bar"])
    {'and': ['foo', 'bar']}
    """
    if isinstance(terms, tuple) and len(terms) == 1:
        terms = terms[0]
    elif not isinstance(terms, list):
        terms = list(terms)
    return {"and": terms}


def OR(*terms) -> Expression:
    """
    >>> OR("foo", "bar")
    {'or': ['foo', 'bar']}
    >>> OR(["foo", "bar"])
    {'or': ['foo', 'bar']}
    """
    if isinstance(terms, tuple) and len(terms) == 1:
        terms = terms[0]
    elif not isinstance(terms, list):
        terms = list(terms)
    return {"or": terms}


def NOT(term: Term) -> Expression:
    return {"not": term}


# For those who prefer to use the Lucene syntax
Must = AND
Should = OR


def MustNot(*terms):
    return NOT(Must(*terms))


def convert_to_v2(terms_v1: TermsV1) -> Expression:
    """
    Convert v1 filters to v2 filters
    """
    return AND(terms_v1)


def andify(expression: Expression) -> Expression:
    """
    Convert an expression to a canonical form where all the 'or' operators
    are replaced by 'and' operators with 'not' operators inside.

    >>> andify({"or": ["/entity/GPE/Sevilla", "/entity/GPE/Madrid"]})
    {'and': [{'not': '/entity/GPE/Sevilla'}, {'not': '/entity/GPE/Madrid'}]}

    >>> andify({"or": ["/entity/GPE/Sevilla", {"not": "/entity/GPE/Madrid"}]})
    {'and': [{'not': '/entity/GPE/Sevilla'}, {'not': {'not': '/entity/GPE/Madrid'}}]}
    """
    if isinstance(expression, str):
        return expression
    if "or" in expression:
        return {"and": [{"not": andify(subexpr)} for subexpr in expression["or"]]}
    elif "and" in expression:
        return {"and": [andify(subexpr) for subexpr in expression["and"]]}
    elif "not" in expression:
        return {"not": andify(expression["not"])}
    else:
        return expression


#######################


class SearchRequest(BaseModel):
    query: str
    # Supports either v1 (list of terms) and v2 (expression)
    filters: Optional[Union[TermsV1, Expression]] = Field(
        default=None,
        title="Filters",
        description="Filters to apply to the query. It can be either a list of terms or an expression. Expressions are more powerful and allow to combine terms with 'and', 'or' and 'not' operators. See the documentation for more details.",
    )

    @validator("filters", pre=True)
    def validate_expression(cls, v):
        if isinstance(v, list):
            v = convert_to_v2(v)
        jsonschema.validate(v, EXPRESSION_JSON_SCHEMA)
        return v


def main():
    # V1 filters
    # label/DOC/Article AND entity/GPE/Sevilla
    req1 = SearchRequest(
        query="temperature",
        filters=["/entity/GPE/Sevilla", "/label/DOC/Article"],
    )
    assert req1.filters == AND("/entity/GPE/Sevilla", "/label/DOC/Article")

    # V2 filters
    # label/DOC/Article AND entity/GPE/Sevilla
    req2 = SearchRequest(
        query="temperature",
        filters=AND("/entity/GPE/Sevilla", "/label/DOC/Article"),
    )

    # entity/GPE/Sevilla OR entity/GPE/Madrid
    req3 = SearchRequest(
        query="temperature",
        filters=OR("/entity/GPE/Sevilla", "/entity/GPE/Madrid"),
    )

    # NOT entity/GPE/Sevilla
    req4 = SearchRequest(
        query="temperature",
        filters=NOT("/entity/GPE/Sevilla"),
    )

    #  5. (v2) NOT (entity/GPE/Sevilla AND entity/GPE/Madrid)
    req5 = SearchRequest(
        query="temperature",
        filters=NOT(AND("/entity/GPE/Sevilla", "/entity/GPE/Madrid")),
    )

    # 6. (v2) NOT (entity/GPE/Sevilla OR entity/GPE/Madrid)
    req6 = SearchRequest(
        query="temperature",
        filters=NOT(OR("/entity/GPE/Sevilla", "/entity/GPE/Madrid")),
    )

    # 7. (v2) label/DOC/Article AND (entity/GPE/Sevilla OR entity/GPE/Madrid)
    req7 = SearchRequest(
        query="temperature",
        filters=AND(
            "/label/DOC/Article",
            OR("/entity/GPE/Sevilla", "/entity/GPE/Madrid"),
        ),
    )

    # 8. (v2) label/DOC/Article AND (entity/GPE/Sevilla OR entity/GPE/Madrid) AND NOT entity/Company/Apple
    req8 = SearchRequest(
        query="temperature",
        filters=AND(
            "/label/DOC/Article",
            OR("/entity/GPE/Sevilla", "/entity/GPE/Madrid"),
            NOT("/entity/Company/Apple"),
        ),
    )

    # 9. (v2) label/DOC/Article OR (entity/GPE/Sevilla AND entity/GPE/Madrid) OR NOT (entity/Company/Apple AND entity/Company/Google)
    req9 = SearchRequest(
        query="temperature",
        filters=OR(
            "/label/DOC/Article",
            AND("/entity/GPE/Sevilla", "/entity/GPE/Madrid"),
            NOT(AND("/entity/Company/Apple", "/entity/Company/Google")),
        ),
    )

    for expr, req in [
        ("/label/DOC/Article AND /entity/GPE/Sevilla", req1),
        ("/label/DOC/Article AND /entity/GPE/Sevilla", req2),
        ("/entity/GPE/Sevilla OR /entity/GPE/Madrid", req3),
        ("NOT /entity/GPE/Sevilla", req4),
        ("NOT (/entity/GPE/Sevilla AND /entity/GPE/Madrid)", req5),
        ("NOT (/entity/GPE/Sevilla OR /entity/GPE/Madrid)", req6),
        (
            "/label/DOC/Article AND (/entity/GPE/Sevilla OR /entity/GPE/Madrid)",
            req7,
        ),
        (
            "/label/DOC/Article AND (/entity/GPE/Sevilla OR /entity/GPE/Madrid) AND NOT /entity/Company/Apple",
            req8,
        ),
        (
            "/label/DOC/Article OR (/entity/GPE/Sevilla AND /entity/GPE/Madrid) OR NOT (/entity/Company/Apple AND /entity/Company/Google)",
            req9,
        ),
    ]:
        print(f"Expression: {expr}")
        print(f"Search query (json):")
        print(req.json(exclude_unset=True, indent=2))
        print("------------------")


if __name__ == "__main__":
    main()

"""
Expression: /label/DOC/Article AND /entity/GPE/Sevilla
Search query (json):
{
  "query": "temperature",
  "filters": {
    "and": [
      "/entity/GPE/Sevilla",
      "/label/DOC/Article"
    ]
  }
}
------------------
Expression: /label/DOC/Article AND /entity/GPE/Sevilla
Search query (json):
{
  "query": "temperature",
  "filters": {
    "and": [
      "/entity/GPE/Sevilla",
      "/label/DOC/Article"
    ]
  }
}
------------------
Expression: /entity/GPE/Sevilla OR /entity/GPE/Madrid
Search query (json):
{
  "query": "temperature",
  "filters": {
    "or": [
      "/entity/GPE/Sevilla",
      "/entity/GPE/Madrid"
    ]
  }
}
------------------
Expression: NOT /entity/GPE/Sevilla
Search query (json):
{
  "query": "temperature",
  "filters": {
    "not": "/entity/GPE/Sevilla"
  }
}
------------------
Expression: NOT (/entity/GPE/Sevilla AND /entity/GPE/Madrid)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "not": {
      "and": [
        "/entity/GPE/Sevilla",
        "/entity/GPE/Madrid"
      ]
    }
  }
}
------------------
Expression: NOT (/entity/GPE/Sevilla OR /entity/GPE/Madrid)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "not": {
      "or": [
        "/entity/GPE/Sevilla",
        "/entity/GPE/Madrid"
      ]
    }
  }
}
------------------
Expression: /label/DOC/Article AND (/entity/GPE/Sevilla OR /entity/GPE/Madrid)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "and": [
      "/label/DOC/Article",
      {
        "or": [
          "/entity/GPE/Sevilla",
          "/entity/GPE/Madrid"
        ]
      }
    ]
  }
}
------------------
Expression: /label/DOC/Article AND (/entity/GPE/Sevilla OR /entity/GPE/Madrid) AND NOT /entity/Company/Apple
Search query (json):
{
  "query": "temperature",
  "filters": {
    "and": [
      "/label/DOC/Article",
      {
        "or": [
          "/entity/GPE/Sevilla",
          "/entity/GPE/Madrid"
        ]
      },
      {
        "not": "/entity/Company/Apple"
      }
    ]
  }
}
------------------
Expression: /label/DOC/Article OR (/entity/GPE/Sevilla AND /entity/GPE/Madrid) OR NOT (/entity/Company/Apple AND /entity/Company/Google)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "or": [
      "/label/DOC/Article",
      {
        "and": [
          "/entity/GPE/Sevilla",
          "/entity/GPE/Madrid"
        ]
      },
      {
        "not": {
          "and": [
            "/entity/Company/Apple",
            "/entity/Company/Google"
          ]
        }
      }
    ]
  }
}
------------------
"""
