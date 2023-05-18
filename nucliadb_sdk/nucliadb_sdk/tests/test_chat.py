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


def test_chat_resource(docs_dataset, sdk: nucliadb_sdk.NucliaSDK):
    result = sdk.chat(kbid=docs_dataset, query="Nuclia loves Semantic Search")
    assert result.learning_id == "00"
    assert result.answer == "valid answer  to"
    assert len(result.result.resources) == 9
    assert result.relations
    assert len(result.relations.entities["Nuclia"].related_to) == 18
