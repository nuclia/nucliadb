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
from typing import Optional

from pydantic import BaseModel


class Docstring(BaseModel):
    doc: str
    examples: Optional[str] = None


SEARCH = Docstring(
    doc="""Search in your Knowledge Box""",
    examples="""
Advanced search on the full text index
>>> resp = sdk.search(kbid="mykbid", advanced_query="text:SRE OR text:DevOps", features=["document"])
>>> rid = resp.fulltext.results[0].rid
>>> resp.resources[rid].title
The Site Reliability Workbook.pdf
""",
)

FIND = Docstring(
    doc="""Find documents in your Knowledge Box""",
    examples="""
Find documents matching a query
>>> resp = sdk.find(kbid="mykbid", query="Very experienced candidates with Rust experience")
>>> resp.resources.popitem().title
Graydon_Hoare.cv.pdf

Filter down by country and increase accuracy of results
>>> content = FindRequest(query="Very experienced candidates with Rust experience", filters=["/l/country/Spain"], min_score=2.5)
>>> resp = sdk.find(kbid="mykbid", content=content)
>>> resp.resources.popitem().title
http://github.com/hermeGarcia
""",  # noqa
)

CHAT = Docstring(
    doc="""Chat with your Knowledge Box""",
    examples="""
Get an answer for a question that is part of the data in the Knowledge Box
>>> resp = sdk.chat(kbid="mykbid", query="Will France be in recession in 2023?")
>>> print(resp.answer)
Yes, according to the provided context, France is expected to be in recession in 2023.

You can use the `content` parameter to pass a `ChatRequest` object
>>> content = ChatRequest(query="Who won the 2018 football World Cup?")
>>> resp = sdk.chat(kbid="mykbid", content=content)
>>> print(resp.answer)
France won the 2018 football World Cup.
""",
)
