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
Proposal:

- Only support for advanced filtering.
- Filters field in query model supports more complex queries: FiltersV2
- Old filters are supported
- Both filters are mutually exclusive
- FiltersV1 will be translated to FiltersV2 expressions internally

Rollout plan:
- Add support for advanced filtering in the IndexNode
- Make current filters work with advanced filtering
- Extend the API to support advanced filtering
"""
from typing import List, Optional, Union

from pydantic import BaseModel


class Expression(BaseModel):
    must: Optional[List[Union[str, "Expression"]]] = None
    should: Optional[List[Union[str, "Expression"]]] = None
    must_not: Optional[List[Union[str, "Expression"]]] = None


Expression.update_forward_refs()

## Helper functions that make it nicer to create expressions


def Must(args):
    return Expression(must=args)


def Should(args):
    return Expression(should=args)


def MustNot(args):
    return Expression(must_not=args)


def Bool(must=None, should=None, must_not=None):
    return Expression(must=must, should=should, must_not=must_not)


def convert_to_v2(filters: List[str]) -> Expression:
    """
    Convert v1 filters to v2 filters
    """
    return Must(filters)


#######################


class SearchRequest(BaseModel):
    query: str
    # Filters query should support both v1 and v2
    filters: Optional[Union[list[str], Expression]] = None


def main():
    # label/DOC/Article AND entity/GPE/Sevilla
    req1 = SearchRequest(
        query="temperature",
        filters=["/entity/GPE/Sevilla", "/label/DOC/Article"],
    )

    # label/DOC/Article AND entity/GPE/Sevilla
    req2 = SearchRequest(
        query="temperature",
        filters=Expression(
            must=["/entity/GPE/Sevilla", "/label/DOC/Article"],
        ),
    )

    # entity/GPE/Sevilla OR entity/GPE/Madrid
    req3 = SearchRequest(
        query="temperature",
        filters=Should(["/entity/GPE/Sevilla", "/entity/GPE/Madrid"]),
    )

    # NOT entity/GPE/Sevilla
    req4 = SearchRequest(
        query="temperature",
        filters=MustNot(["/entity/GPE/Sevilla"]),
    )

    #  5. (v2) NOT (entity/GPE/Sevilla AND entity/GPE/Madrid)
    req5 = SearchRequest(
        query="temperature",
        filters=MustNot(["/entity/GPE/Sevilla", "/entity/GPE/Madrid"]),
    )

    # 6. (v2) NOT (entity/GPE/Sevilla OR entity/GPE/Madrid)
    req6 = SearchRequest(
        query="temperature",
        filters=MustNot([Should(["/entity/GPE/Sevilla", "/entity/GPE/Madrid"])]),
    )

    # 7. (v2) label/DOC/Article AND (entity/GPE/Sevilla OR entity/GPE/Madrid)
    req7 = SearchRequest(
        query="temperature",
        filters=Bool(
            should=["/entity/GPE/Sevilla", "/entity/GPE/Madrid"],
            must=["/label/DOC/Article"],
        ),
    )

    # 8. (v2) label/DOC/Article AND (entity/GPE/Sevilla OR entity/GPE/Madrid) AND NOT entity/Company/Apple
    req8 = SearchRequest(
        query="temperature",
        filters=Bool(
            should=["/entity/GPE/Sevilla", "/entity/GPE/Madrid"],
            must=["/label/DOC/Article"],
            must_not=["/entity/Company/Apple"],
        ),
    )

    # 9. (v2) label/DOC/Article OR (entity/GPE/Sevilla AND entity/GPE/Madrid) OR NOT (entity/Company/Apple AND entity/Company/Google)
    req9 = SearchRequest(
        query="temperature",
        filters=Should(
            [
                "/label/DOC/Article",
                Must(["/entity/GPE/Sevilla", "/entity/GPE/Madrid"]),
                MustNot(["/entity/Company/Apple", "/entity/Company/Google"]),
            ]
        ),
    )

    from pprint import pprint

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
  "filters": [
    "/entity/GPE/Sevilla",
    "/label/DOC/Article"
  ]
}
------------------
Expression: /label/DOC/Article AND /entity/GPE/Sevilla
Search query (json):
{
  "query": "temperature",
  "filters": {
    "must": [
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
    "should": [
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
    "must_not": [
      "/entity/GPE/Sevilla"
    ]
  }
}
------------------
Expression: NOT (/entity/GPE/Sevilla AND /entity/GPE/Madrid)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "must_not": [
      "/entity/GPE/Sevilla",
      "/entity/GPE/Madrid"
    ]
  }
}
------------------
Expression: NOT (/entity/GPE/Sevilla OR /entity/GPE/Madrid)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "must_not": [
      {
        "should": [
          "/entity/GPE/Sevilla",
          "/entity/GPE/Madrid"
        ]
      }
    ]
  }
}
------------------
Expression: /label/DOC/Article AND (/entity/GPE/Sevilla OR /entity/GPE/Madrid)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "must": [
      "/label/DOC/Article"
    ],
    "should": [
      "/entity/GPE/Sevilla",
      "/entity/GPE/Madrid"
    ],
    "must_not": null
  }
}
------------------
Expression: /label/DOC/Article AND (/entity/GPE/Sevilla OR /entity/GPE/Madrid) AND NOT /entity/Company/Apple
Search query (json):
{
  "query": "temperature",
  "filters": {
    "must": [
      "/label/DOC/Article"
    ],
    "should": [
      "/entity/GPE/Sevilla",
      "/entity/GPE/Madrid"
    ],
    "must_not": [
      "/entity/Company/Apple"
    ]
  }
}
------------------
Expression: /label/DOC/Article OR (/entity/GPE/Sevilla AND /entity/GPE/Madrid) OR NOT (/entity/Company/Apple AND /entity/Company/Google)
Search query (json):
{
  "query": "temperature",
  "filters": {
    "should": [
      "/label/DOC/Article",
      {
        "must": [
          "/entity/GPE/Sevilla",
          "/entity/GPE/Madrid"
        ]
      },
      {
        "must_not": [
          "/entity/Company/Apple",
          "/entity/Company/Google"
        ]
      }
    ]
  }
}
------------------
"""