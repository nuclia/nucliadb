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

import nucliadb_sdk
from nucliadb_models.resource import KnowledgeBoxObj


def test_create_kb(sdk: nucliadb_sdk.NucliaDB):
    kb: KnowledgeBoxObj = sdk.create_knowledge_box(slug="hola")
    assert sdk.get_knowledge_box(kbid=kb.uuid) is not None
    assert sdk.get_knowledge_box_by_slug(slug="hola") is not None

    with pytest.raises(nucliadb_sdk.exceptions.ConflictError):
        sdk.create_knowledge_box(slug="hola")
