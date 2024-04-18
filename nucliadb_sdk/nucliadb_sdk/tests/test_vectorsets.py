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


import nucliadb_sdk
from nucliadb_models.vectors import VectorSet, VectorSimilarity


def test_vectorsets_api(sdk: nucliadb_sdk.NucliaDB, kb):
    vectorset = VectorSet(
        semantic_model="foo",
        semantic_vector_similarity=VectorSimilarity.DOT,
        semantic_vector_size=100,
        semantic_threshold=0.5,
        semantic_matryoshka_dimensions=[100, 200, 300],
    )
    sdk.create_vectorset(kbid=kb.uuid, vectorset="foo", content=vectorset)
    sdk.create_vectorset(kbid=kb.uuid, vectorset="bar", content=vectorset)
    vectorsets = sdk.list_vectorsets(kbid=kb.uuid)
    assert len(vectorsets.vectorsets) == 2
    assert vectorsets.vectorsets["foo"] == vectorset
    assert vectorsets.vectorsets["bar"] == vectorset

    sdk.delete_vectorset(kbid=kb.uuid, vectorset="foo")

    vectorsets = sdk.list_vectorsets(kbid=kb.uuid)
    assert len(vectorsets.vectorsets) == 1
