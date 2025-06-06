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


def test_create_kb(sdk: nucliadb_sdk.NucliaDB):
    kb: KnowledgeBoxObj = sdk.create_knowledge_box(slug="hola")
    assert sdk.get_knowledge_box(kbid=kb.uuid) is not None
    assert sdk.get_knowledge_box_by_slug(slug="hola") is not None

    with pytest.raises(nucliadb_sdk.exceptions.ConflictError):
        sdk.create_knowledge_box(slug="hola")
