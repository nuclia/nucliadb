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

from enum import Enum
from typing import Union

from pydantic import BaseModel


class ExternalIndexProviderType(str, Enum):
    """
    Enum for the different external index providers.
    For now only Pinecone is supported, but we may add more in the future.
    """

    PINECONE = "pinecone"


class ExternalIndexProviderBase(BaseModel):
    type: ExternalIndexProviderType


class PineconeIndexProvider(ExternalIndexProviderBase):
    type: ExternalIndexProviderType = ExternalIndexProviderType.PINECONE
    api_key: str


ExternalIndexProvider = Union[PineconeIndexProvider,]
