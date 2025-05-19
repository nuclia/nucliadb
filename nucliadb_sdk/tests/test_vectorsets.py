# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

import nucliadb_sdk
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_sdk.tests.fixtures import NucliaFixture
from nucliadb_sdk.v2.exceptions import ConflictError


def test_vectorsets(nucliadb: NucliaFixture, sdk: nucliadb_sdk.NucliaDB, kb: KnowledgeBoxObj):
    # this test don't have a proper learning mock set up, adding a vectorset
    # involves learning updating learning_config and returning the new semantic
    # model information and, as we can't unittest.patch either, we just test the
    # endpoint respond

    vectorsets = sdk.list_vector_sets(kbid=kb.uuid)
    assert len(vectorsets.vectorsets) == 1

    existing_vectorset = vectorsets.vectorsets[0].id
    assert existing_vectorset

    # can't add an already existing vectorset
    with pytest.raises(ConflictError):
        sdk.add_vector_set(kbid=kb.uuid, vectorset_id=existing_vectorset)

    # can't delete the last vectorset
    with pytest.raises(ConflictError):
        sdk.delete_vector_set(kbid=kb.uuid, vectorset_id=existing_vectorset)
