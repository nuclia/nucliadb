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

import warnings
from typing import Optional

from pydantic import BaseModel


class KBConfiguration(BaseModel):
    def __init__(self, **data):
        warnings.warn("KBConfiguration model is deprecated", DeprecationWarning)
        super().__init__(**data)

    # Do not touch this model synced on Processing side
    semantic_model: Optional[str] = None
    generative_model: Optional[str] = None
    ner_model: Optional[str] = None
    anonymization_model: Optional[str] = None
    visual_labeling: Optional[str] = None
