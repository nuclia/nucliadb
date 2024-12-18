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

"""NucliaDB fixtures

This module should never be imported directly, as it contains a dangerous mix of
imports. It's done like this to avoid having to import all files in the conftest.

"""

# hacks and magic things with pytest to implement deploy mode parametrization

from .magic import *  # noqa

# components and deployment modes

from .reader import *  # noqa
from .writer import *  # noqa

# subcomponents

from .common import *  # type: ignore # noqa
from .maindb import *  # noqa
from .node import *  # noqa

# useful resources for tests (KBs...)

from .resources import *  # noqa
