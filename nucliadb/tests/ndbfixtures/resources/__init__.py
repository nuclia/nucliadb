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

# ruff: noqa: F401

from .cookie_tale import cookie_tale_resource
from .datafusion import datafusion_resource
from .full import full_resource
from .knowledge_graph import entity_graph, graph_resource, kb_with_entity_graph, knowledge_graph
from .knowledgebox import knowledgebox, standalone_knowledgebox
from .simples import resource, simple_resources
from .smb_wonder import smb_wonder_resource
