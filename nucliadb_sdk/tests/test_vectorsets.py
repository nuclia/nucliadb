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
import os

import pytest

import nucliadb_sdk
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_sdk.v2.exceptions import ConflictError


def test_add_and_delete_vectorset(sdk: nucliadb_sdk.NucliaDB, kb: KnowledgeBoxObj):
    # this test don't have a proper learning mock set up, adding a vectorset
    # involves learning updating learning_config and returning the new semantic
    # model information and, as we can't unittest.patch either, we just test the
    # endpoint respond

    existing_vectorset = os.environ.get("TEST_SENTENCE_ENCODER", "multilingual")
    # can't add an already existing vectorset
    with pytest.raises(ConflictError):
        sdk.add_vector_set(kbid=kb.uuid, vectorset_id=existing_vectorset)

    # can't delete the last vectorset
    with pytest.raises(ConflictError):
        sdk.delete_vector_set(kbid=kb.uuid, vectorset_id=existing_vectorset)
