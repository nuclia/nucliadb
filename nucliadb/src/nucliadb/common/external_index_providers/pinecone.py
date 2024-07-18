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
import logging
from typing import Iterator

from nucliadb.common.external_index_providers.base import (
    ExternalIndexManager,
    ExternalIndexProviderType,
    QueryResults,
    TextBlockMatch,
)
from nucliadb.common.ids import VectorId
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_telemetry.metrics import Observer
from nucliadb_utils.aiopynecone.models import QueryResponse
from nucliadb_utils.aiopynecone.models import Vector as PineconeVector
from nucliadb_utils.utilities import get_pinecone

logger = logging.getLogger(__name__)

manager_observer = Observer("pinecone_index_manager", labels={"operation": ""})


DISCARDED_LABEL_PREFIXES = [
    # NER-related labels are not supported in the Pinecone integration because right now
    # the number of detected entities is unbounded and may exceed the vector metadata size limit.
    "/e/",
    # Processing status labels are only needed for the catalog endpoint.
    "/n/s",
]


class PineconeQueryResults(QueryResults):
    type: ExternalIndexProviderType = ExternalIndexProviderType.PINECONE
    results: QueryResponse

    def iter_matching_text_blocks(self) -> Iterator[TextBlockMatch]:
        for order, matching_vector in enumerate(self.results.matches):
            try:
                vector_id = VectorId.from_string(matching_vector.id)
            except ValueError:
                logger.error(f"Invalid Pinecone vector id: {matching_vector.id}")
                continue
            yield TextBlockMatch(
                id=matching_vector.id,
                resource_id=vector_id.field_id.rid,
                field_id=vector_id.field_id.full(),
                score=matching_vector.score,
                order=order,
                position_start=vector_id.vector_start,
                position_end=vector_id.vector_end,
                subfield_id=vector_id.field_id.subfield_id,
                index=vector_id.index,
                text=None,  # To be filled by the results hydrator
            )


class PineconeIndexManager(ExternalIndexManager):
    type = ExternalIndexProviderType.PINECONE

    def __init__(
        self,
        kbid: str,
        api_key: str,
        index_host: str,
        upsert_parallelism: int = 3,
        delete_parallelism: int = 2,
        upsert_timeout: float = 10.0,
        delete_timeout: float = 10.0,
        query_timeout: float = 10.0,
    ):
        super().__init__(kbid=kbid)
        assert api_key != ""
        self.api_key = api_key
        assert index_host != ""
        self.index_host = index_host
        pinecone = get_pinecone()
        self.data_plane = pinecone.data_plane(api_key=self.api_key, index_host=self.index_host)
        self.upsert_parallelism = upsert_parallelism
        self.delete_parallelism = delete_parallelism
        self.upsert_timeout = upsert_timeout
        self.delete_timeout = delete_timeout
        self.query_timeout = query_timeout

    async def _delete_resource(self, resource_uuid: str) -> None:
        with manager_observer({"operation": "delete_by_resource_prefix"}):
            await self.data_plane.delete_by_id_prefix(
                id_prefix=resource_uuid,
                max_parallel_batches=self.delete_parallelism,
                batch_timeout=self.delete_timeout,
            )

    async def _index_resource(self, resource_uuid: str, index_data: Resource) -> None:
        # First off, delete any previously existing vectors with the same field prefixes.
        field_prefixes_to_delete = set()
        for to_delete in list(index_data.sentences_to_delete) + list(index_data.paragraphs_to_delete):
            try:
                resource_uuid, field_type, field_id = to_delete.split("/")[:3]
                field_prefixes_to_delete.add(f"{resource_uuid}/{field_type}/{field_id}")
            except ValueError:
                continue
        with manager_observer({"operation": "delete_by_field_prefix"}):
            for prefix in field_prefixes_to_delete:
                await self.data_plane.delete_by_id_prefix(
                    id_prefix=prefix,
                    max_parallel_batches=self.delete_parallelism,
                    batch_timeout=self.delete_timeout,
                )
        access_groups = None
        if index_data.HasField("security"):
            access_groups = list(set(index_data.security.access_groups))
        # Iterate over paragraphs and fetch paragraph data
        resource_labels = set(index_data.labels)
        paragraphs_data = {}
        for field_id, text_info in index_data.texts.items():
            field_labels = set(text_info.labels)
            field_paragraphs = index_data.paragraphs.get(field_id)
            if field_paragraphs is None:
                continue
            for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
                paragraph_labels = set(paragraph.labels).union(field_labels).union(resource_labels)
                paragraphs_data[paragraph_id] = {
                    "labels": list(paragraph_labels),
                }
        # Iterate sentences now, and compute the list of vectors to upsert
        vectors: list[PineconeVector] = []
        for _, index_paragraphs in index_data.paragraphs.items():
            for index_paragraph_id, index_paragraph in index_paragraphs.paragraphs.items():
                paragraph_data = paragraphs_data.get(index_paragraph_id) or {}
                for sentence_id, vector_sentence in index_paragraph.sentences.items():
                    sentence_labels = (
                        set(index_paragraph.labels)
                        .union(paragraph_data.get("labels") or set())
                        .union(resource_labels)
                    )
                    # Filter out discarded labels
                    sentence_labels = {
                        label
                        for label in sentence_labels
                        if not any(label.startswith(prefix) for prefix in DISCARDED_LABEL_PREFIXES)
                    }
                    vector_metadata = {
                        "labels": list(sentence_labels),
                    }
                    if access_groups is not None:
                        vector_metadata["access_groups"] = access_groups
                    pc_vector = PineconeVector(
                        id=sentence_id,
                        values=list(vector_sentence.vector),
                        metadata=vector_metadata,
                    )
                    vectors.append(pc_vector)
        if len(vectors) == 0:  # pragma: no cover
            return
        with manager_observer({"operation": "upsert_in_batches"}):
            await self.data_plane.upsert_in_batches(
                vectors=vectors,
                max_parallel_batches=self.upsert_parallelism,
                batch_timeout=self.upsert_timeout,
            )

    async def _query(self, request: SearchRequest) -> PineconeQueryResults:
        top_k = (request.page_number + 1) * request.result_per_page
        query_results = await self.data_plane.query(
            vector=list(request.vector),
            top_k=top_k,
            include_values=False,
            include_metadata=True,
            filter=None,  # TODO: add filter support
            timeout=self.query_timeout,
        )
        return PineconeQueryResults(results=query_results)
