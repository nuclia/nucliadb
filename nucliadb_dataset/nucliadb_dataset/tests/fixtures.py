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

import pytest

from nucliadb_sdk.entities import Entity
from nucliadb_sdk.knowledgebox import KnowledgeBox
from nucliadb_sdk.labels import LabelType


@pytest.fixture(scope="function")
def upload_data_field_classification(knowledgebox: KnowledgeBox):
    knowledgebox.set_labels("labelset1", ["label1", "label2"], LabelType.RESOURCES)
    knowledgebox.upload(
        "doc1", text="This is my lovely text", labels=["labelset1/label1"]
    )
    knowledgebox.upload(
        "doc2",
        text="This is my lovely text2",
        labels=["labelset1/label1", "labelset1/label2"],
    )


@pytest.fixture(scope="function")
def upload_data_token_classification(knowledgebox: KnowledgeBox):
    knowledgebox.set_entities("PERSON", ["Ramon", "Carmen Iniesta", "Eudald Camprubi"])
    knowledgebox.upload(
        "doc1",
        text="Ramon This is my lovely text",
        entities=[Entity(type="PERSON", value="Ramon", positions=[(0, 5)])],
    )
    knowledgebox.upload(
        "doc2",
        text="Carmen Iniesta shows an amazing classifier to Eudald Camprubi",
        entities=[
            Entity(type="PERSON", value="Carmen Iniesta", positions=[(0, 14)]),
            Entity(type="PERSON", value="Eudald Camprubi", positions=[(46, 61)]),
        ],
    )
