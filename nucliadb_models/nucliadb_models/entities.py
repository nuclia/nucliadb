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

from typing import Dict, List, Optional

from pydantic import BaseModel


class Entity(BaseModel):
    value: str
    merged: bool = False
    represents: List[str] = []


class EntitiesGroup(BaseModel):
    entities: Dict[str, Entity] = {}
    title: Optional[str] = None
    color: Optional[str] = None

    # custom = true if they have been created by the user.
    custom: bool = False


class KnowledgeBoxEntities(BaseModel):
    uuid: str
    groups: Dict[str, EntitiesGroup] = {}
