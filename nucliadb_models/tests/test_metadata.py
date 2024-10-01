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
        metadata.UserMetadata(relations=["my-wrong-relation"])


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
