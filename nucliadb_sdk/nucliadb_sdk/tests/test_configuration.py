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
from nucliadb_models.configuration import KBConfiguration


def test_configuration(sdk: nucliadb_sdk.NucliaDB):
    # Create a KB with dot similarity
    kb = sdk.create_knowledge_box(slug="config")
    assert kb is not None

    # Add configuration with different similarities
    sdk.set_configuration(
        kbid=kb.uuid,
        semantic_model="test1",
        generative_model="test2",
    )

    config: KBConfiguration = sdk.get_configuration(kbid=kb.uuid)
    assert config.semantic_model == "test1"
    assert config.generative_model == "test2"
    assert config.anonymization_model is None
