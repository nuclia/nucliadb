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
import abc
import logging
from enum import Enum
from typing import Any

from pydantic import BaseModel

from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.noderesources_pb2 import Resource

logger = logging.getLogger(__name__)


class ExternalIndexProviderType(Enum):
    """
    Enum for the different external index providers.
    For now only Pinecone is supported, but we may add more in the future.
    """

    PINECONE = "pinecone"


class QueryResults(BaseModel):
    """
    Model for the results of a query to an external index provider.
    Must be subclassed by the specific external index provider.
    """

    type: ExternalIndexProviderType
    results: Any


class ExternalIndexManager(abc.ABC, metaclass=abc.ABCMeta):
    """
    Base class for the external index providers. Must be subclassed by the specific external index provider.
    """

    type: ExternalIndexProviderType

    def __init__(self, kbid: str):
        self.kbid = kbid

    async def delete_resource(self, resource_uuid: str) -> None:
        """
        Deletes a resource from the external index provider.
        """
        logger.info(
            "Deleting resource to external index",
            extra={
                "kbid": self.kbid,
                "rid": resource_uuid,
                "provider": self.type.value,
            },
        )
        await self._delete_resource(resource_uuid)

    async def index_resource(self, resource_uuid: str, resource_data: Resource) -> None:
        """
        Indexes a resource to the external index provider.
        """
        logger.info(
            "Indexing resource to external index",
            extra={
                "kbid": self.kbid,
                "rid": resource_uuid,
                "provider": self.type.value,
            },
        )
        await self._index_resource(resource_uuid, resource_data)

    async def query(self, request: SearchRequest) -> QueryResults:
        """
        Queries the external index provider and returns the results.
        """
        logger.info(
            "Querying external index",
            extra={
                "kbid": self.kbid,
                "provider": self.type.value,
            },
        )
        return await self._query(request)

    @abc.abstractmethod
    async def _delete_resource(self, resource_uuid: str) -> None:  # pragma: no cover
        """
        Makes sure that all vectors associated with the resource are deleted from the external index provider.
        """
        ...

    @abc.abstractmethod
    async def _index_resource(
        self, resource_uuid: str, resource_data: Resource
    ) -> None:  # pragma: no cover
        """
        Adapts the Resource (aka brain) to the external index provider's index format and indexes it.
        """
        ...

    @abc.abstractmethod
    async def _query(self, request: SearchRequest) -> QueryResults:  # pragma: no cover
        """
        Adapts the Nucliadb's search request to the external index provider's query format and returns the results.
        """
        ...
