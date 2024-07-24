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
from typing import Any, Iterator, Optional

from pydantic import BaseModel

from nucliadb_models.external_index_providers import ExternalIndexProviderType
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_telemetry.metrics import Observer

logger = logging.getLogger(__name__)

manager_observer = Observer("external_index_manager", labels={"operation": "", "provider": ""})


class TextBlockMatch(BaseModel):
    """
    Model a text block/paragraph retrieved from an external index with all the information
    needed in order to later hydrate retrieval results.
    """

    id: str
    resource_id: str
    field_id: str
    subfield_id: Optional[str] = None
    index: Optional[int] = None
    position_start: int
    position_end: int
    position_start_seconds: list[int] = []
    position_end_seconds: list[int] = []
    page_number: Optional[int] = None
    score: float
    order: int
    page_with_visual: bool = False
    is_a_table: bool = False
    representation_file: Optional[str] = None
    paragraph_labels: list[str] = []
    field_labels: list[str] = []
    text: Optional[str] = None


class QueryResults(BaseModel):
    """
    Model for the results of a query to an external index provider.
    Must be subclassed by the specific external index provider.
    """

    type: ExternalIndexProviderType
    results: Any

    def iter_matching_text_blocks(self) -> Iterator[TextBlockMatch]:
        """
        Iterates over the paragraphs in the results, by decreasing score.
        This should be implemented by the specific external index provider.
        """
        raise NotImplementedError()


class ExternalIndexManager(abc.ABC, metaclass=abc.ABCMeta):
    """
    Base class for the external index providers. Must be subclassed by the specific external index provider.
    """

    type: ExternalIndexProviderType

    def __init__(self, kbid: str):
        self.kbid = kbid

    @classmethod
    def get_index_name(cls, kbid: str, vectorset_id: str) -> str:  # pragma: no cover
        """
        Returns the name of the index in the external index provider.
        """
        raise NotImplementedError()

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
        with manager_observer({"operation": "delete_resource", "provider": self.type.value}):
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
        with manager_observer({"operation": "index_resource", "provider": self.type.value}):
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
        with manager_observer({"operation": "query", "provider": self.type.value}):
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
