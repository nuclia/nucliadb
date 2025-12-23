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
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from nidx_protos.nodereader_pb2 import SearchRequest
from nidx_protos.noderesources_pb2 import Resource
from pydantic import BaseModel

from nucliadb.common.counters import IndexCounts
from nucliadb.common.external_index_providers.exceptions import ExternalIndexingError
from nucliadb.common.ids import ParagraphId
from nucliadb_models.external_index_providers import ExternalIndexProviderType
from nucliadb_models.retrieval import Score
from nucliadb_models.search import SCORE_TYPE, Relations, TextPosition
from nucliadb_protos import resources_pb2
from nucliadb_protos.knowledgebox_pb2 import (
    CreateExternalIndexProviderMetadata,
    StoredExternalIndexProviderMetadata,
)
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_telemetry.metrics import Observer

logger = logging.getLogger(__name__)

manager_observer = Observer("external_index_manager", labels={"operation": "", "provider": ""})


# /k/ocr
_OCR_LABEL = (
    f"/k/{resources_pb2.Paragraph.TypeParagraph.Name(resources_pb2.Paragraph.TypeParagraph.OCR).lower()}"
)
# /k/inception
_INCEPTION_LABEL = (
    f"/k/{resources_pb2.Paragraph.TypeParagraph.Name(resources_pb2.Paragraph.TypeParagraph.OCR).lower()}"
)


@dataclass
class VectorsetExternalIndex:
    """
    Used to indicate to external index managers the required metadata
    in order to create an external index for each vectorset
    """

    vectorset_id: str
    dimension: int
    similarity: VectorSimilarity.ValueType


class ScoredTextBlock(BaseModel):
    paragraph_id: ParagraphId
    score_type: SCORE_TYPE

    scores: list[Score]

    @property
    def score(self) -> float:
        return self.current_score.score

    @property
    def current_score(self) -> Score:
        assert len(self.scores) > 0, "text block matches must be scored"
        return self.scores[-1]


class TextBlockMatch(ScoredTextBlock):
    """
    Model a text block/paragraph retrieved from an external index with all the information
    needed in order to later hydrate retrieval results.
    """

    position: TextPosition
    order: int
    page_with_visual: bool = False
    fuzzy_search: bool
    is_a_table: bool = False
    representation_file: str | None = None
    paragraph_labels: list[str] = []
    field_labels: list[str] = []
    text: str | None = None
    relevant_relations: Relations | None = None

    @property
    def is_an_image(self) -> bool:
        return _OCR_LABEL in self.paragraph_labels or _INCEPTION_LABEL in self.paragraph_labels


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
    supports_rollover: bool = False

    def __init__(self, kbid: str):
        self.kbid = kbid

    @classmethod
    @abc.abstractmethod
    async def create_indexes(
        cls,
        kbid: str,
        create_request: CreateExternalIndexProviderMetadata,
        indexes: list[VectorsetExternalIndex],
    ) -> StoredExternalIndexProviderMetadata: ...

    @classmethod
    @abc.abstractmethod
    async def delete_indexes(
        cls,
        kbid: str,
        stored: StoredExternalIndexProviderMetadata,
    ) -> None: ...

    @abc.abstractmethod
    async def rollover_create_indexes(
        self, stored: StoredExternalIndexProviderMetadata
    ) -> StoredExternalIndexProviderMetadata:  # pragma: no cover
        """
        Creates the indexes for the rollover process.
        In the event of an error, it should rollback any left over indexes.
        Returns a modified version of the stored external index provider metadata with the new indexes for the rollover.
        """
        ...

    @abc.abstractmethod
    async def rollover_cutover_indexes(self) -> None:  # pragma: no cover
        """
        Cutover the indexes for the rollover process.
        After this operation, the new indexes should be used for queries and the old ones should be deleted.
        """
        ...

    @classmethod
    def get_index_name(cls) -> str:  # pragma: no cover
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

    async def index_resource(
        self, resource_uuid: str, resource_data: Resource, to_rollover_indexes: bool = False
    ) -> None:
        """
        Indexes a resource to the external index provider.
        """
        if not self.supports_rollover and to_rollover_indexes:
            logger.info(
                "Indexing to rollover indexes not supported",
                extra={
                    "kbid": self.kbid,
                    "rid": resource_uuid,
                    "provider": self.type.value,
                },
            )
            return
        logger.info(
            "Indexing resource to external index",
            extra={
                "kbid": self.kbid,
                "rid": resource_uuid,
                "provider": self.type.value,
                "rollover": to_rollover_indexes,
            },
        )
        with manager_observer({"operation": "index_resource", "provider": self.type.value}):
            try:
                await self._index_resource(
                    resource_uuid, resource_data, to_rollover_indexes=to_rollover_indexes
                )
            except Exception as ex:
                raise ExternalIndexingError() from ex

    async def get_index_counts(self) -> IndexCounts:
        """
        Returns the index counts for the external index provider.
        """
        logger.debug(
            "Getting index counts from external index",
            extra={
                "kbid": self.kbid,
                "provider": self.type.value,
            },
        )
        with manager_observer({"operation": "get_index_counts", "provider": self.type.value}):
            return await self._get_index_counts()

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
        self, resource_uuid: str, resource_data: Resource, to_rollover_indexes: bool = False
    ) -> None:  # pragma: no cover
        """
        Adapts the Resource (aka brain) to the external index provider's index format and indexes it.
        Params:
        - resource_uuid: the resource's UUID
        - resource_data: the resource index data
        - to_rollover_indexes: whether to index to the rollover indexes or the main indexes
        """
        ...

    @abc.abstractmethod
    async def _query(self, request: SearchRequest) -> QueryResults:  # pragma: no cover
        """
        Adapts the Nucliadb's search request to the external index provider's query format and returns the results.
        """
        ...

    @abc.abstractmethod
    async def _get_index_counts(self) -> IndexCounts:  # pragma: no cover
        """
        Returns the index counts for the external index provider.
        """
        ...
