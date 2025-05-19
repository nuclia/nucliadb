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
import logging

from .common import *  # noqa
from .conversation import *  # noqa
from .export_import import *  # noqa
from .external_index_providers import *  # noqa
from .extracted import *  # noqa
from .file import *  # noqa
from .link import *  # noqa
from .metadata import *  # noqa
from .notifications import *  # noqa
from .processing import *  # noqa
from .security import *  # noqa
from .text import *  # noqa
from .writer import *  # noqa

logger = logging.getLogger("nucliadb_models")
