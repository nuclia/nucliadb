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
from nucliadb.search.api.v1.chat import _extract_text
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
)


def find_resource(*, rid, fid, pid, text, score):
    p = FindParagraph(id=pid, text=text, score=score, score_type=SCORE_TYPE.BOTH)
    f = FindField(id=fid, paragraphs={pid: p})
    r = FindResource(id=rid, fields={fid: f})
    return r


def test_extract_text():
    r1 = find_resource(
        rid="r1", fid="f1", pid="p1", text="Second most relevant paragraph", score=5
    )
    r2 = find_resource(
        rid="r2", fid="f2", pid="p2", text="Third most relevant paragraph", score=1
    )
    r3 = find_resource(
        rid="r3", fid="f3", pid="p3", text="Most relevant paragraph", score=20
    )
    find_results = KnowledgeboxFindResults(
        facets=[],
        resources={
            "r1": r1,
            "r2": r2,
            "r3": r3,
        },
    )
    flattened_text = _extract_text(find_results)
    assert flattened_text.split(" \n\n ") == [
        "Most relevant paragraph",
        "Second most relevant paragraph",
        "Third most relevant paragraph",
    ]
