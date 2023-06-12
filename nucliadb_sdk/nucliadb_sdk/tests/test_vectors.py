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


def test_similarity(sdk: nucliadb_sdk.NucliaDB):
    # Create a KB with dot similarity
    kb = sdk.create_knowledge_box(slug="dot", similarity="dot")
    assert kb is not None

    # Add vectorsets with different similarities
    sdk.create_vectorset(
        kbid=kb.uuid, vectorset="cosine", dimension=728, similarity="cosine"
    )
    sdk.create_vectorset(kbid=kb.uuid, vectorset="dot", dimension=728, similarity="dot")

    vectorsets = sdk.list_vectorsets(kbid=kb.uuid)
    assert vectorsets.vectorsets["dot"].similarity.value == "dot"
    assert vectorsets.vectorsets["cosine"].similarity.value == "cosine"
