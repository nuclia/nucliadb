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
#

import pytest
from pydantic_core import ValidationError

from nucliadb_models import metadata
from nucliadb_models.search import AuditMetadataBase


def test_relation_validator():
    metadata.Relation(
        relation=metadata.RelationType.OTHER,
        to=metadata.RelationEntity(value="image-0.jpg", type=metadata.RelationNodeType.RESOURCE),
    )

    with pytest.raises(ValidationError):
        metadata.UserMetadata(relations=["my-wrong-relation"])  # type: ignore


def test_relation_entity_model_validator():
    metadata.RelationEntity(
        value="blue",
        type=metadata.RelationNodeType.LABEL,
    )

    # entities must define a group
    with pytest.raises(ValidationError):
        metadata.RelationEntity(
            value="house",
            type=metadata.RelationNodeType.ENTITY,
        )

    metadata.RelationEntity(
        value="house",
        type=metadata.RelationNodeType.ENTITY,
        group="places",
    )


def test_audit_metadata_base_max_size():
    AuditMetadataBase(audit_metadata={"test": "test1"})
    with pytest.raises(ValidationError):
        AuditMetadataBase(audit_metadata={"test": "a" * 1024 * 10})
