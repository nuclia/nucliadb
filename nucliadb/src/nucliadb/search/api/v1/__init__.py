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
from . import ask  # noqa
from . import catalog  # noqa
from . import feedback  # noqa
from . import find  # noqa
from . import knowledgebox  # noqa
from . import predict_proxy  # noqa
from . import search  # noqa
from . import suggest  # noqa
from . import summarize  # noqa
from .resource import ask as ask_resource  # noqa
from .resource import search as search_resource  # noqa
from .resource import ingestion_agents as ingestion_agents_resource  # noqa
from .router import api  # noqa
