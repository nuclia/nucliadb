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
import asyncio
import logging
from copy import deepcopy
from typing import Any, Iterator, Optional
from uuid import uuid4

import backoff
from cachetools import TTLCache
from nidx_protos.nodereader_pb2 import FilterExpression, SearchRequest
from nidx_protos.noderesources_pb2 import IndexParagraph, Resource, VectorSentence
from pydantic import BaseModel

from nucliadb.common.counters import IndexCounts
from nucliadb.common.external_index_providers.base import (
    ExternalIndexManager,
    ExternalIndexProviderType,
    QueryResults,
    TextBlockMatch,
    VectorsetExternalIndex,
)
from nucliadb.common.external_index_providers.exceptions import ExternalIndexCreationError
from nucliadb.common.ids import ParagraphId, VectorId
from nucliadb_models.search import SCORE_TYPE, TextPosition
from nucliadb_protos import knowledgebox_pb2 as kb_pb2
from nucliadb_protos import utils_pb2
from nucliadb_telemetry.metrics import Observer
from nucliadb_utils.aiopynecone.client import DataPlane, FilterOperator, LogicalOperator
from nucliadb_utils.aiopynecone.exceptions import (
    MetadataTooLargeError,
    PineconeAPIError,
)
from nucliadb_utils.aiopynecone.models import QueryResponse
from nucliadb_utils.aiopynecone.models import Vector as PineconeVector
from nucliadb_utils.utilities import get_endecryptor, get_pinecone

logger = logging.getLogger(__name__)

manager_observer = Observer("pinecone_index_manager", labels={"operation": ""})


DISCARDED_LABEL_PREFIXES = [
    # NER-related labels are not supported in the Pinecone integration because right now
    # the number of detected entities is unbounded and may exceed the vector metadata size limit.
    "/e/",
    # Processing status labels are only needed for the catalog endpoint.
    "/n/s",
]

# To avoid querying the Pinecone API for the same index stats multiple times in a short period of time
COUNTERS_CACHE = TTLCache(maxsize=1024, ttl=60)  # type: ignore


class PineconeQueryResults(QueryResults):
    type: ExternalIndexProviderType = ExternalIndexProviderType.PINECONE
    results: QueryResponse

    def iter_matching_text_blocks(self) -> Iterator[TextBlockMatch]:
        for order, matching_vector in enumerate(self.results.matches):
            try:
                vector_id = VectorId.from_string(matching_vector.id)
                paragraph_id = ParagraphId.from_vector_id(vector_id)
            except ValueError:  # pragma: no cover
                logger.error(f"Invalid Pinecone vector id: {matching_vector.id}")
                continue
            vector_metadata = VectorMetadata.model_validate(matching_vector.metadata)  # noqa
            yield TextBlockMatch(
                paragraph_id=paragraph_id,
                text=None,  # To be filled by the results hydrator
                score=matching_vector.score,
                score_type=SCORE_TYPE.VECTOR,
                order=order,
                fuzzy_search=False,  # semantic search doesn't use fuzziness
                is_a_table=vector_metadata.is_a_table or False,
                page_with_visual=vector_metadata.page_with_visual or False,
                representation_file=vector_metadata.representation_file,
                paragraph_labels=vector_metadata.paragraph_labels or [],
                field_labels=vector_metadata.field_labels or [],
                position=TextPosition(
                    page_number=vector_metadata.page_number,
                    index=vector_id.index or 0,
                    start=paragraph_id.paragraph_start,
                    end=paragraph_id.paragraph_end,
                    start_seconds=list(map(int, vector_metadata.position_start_seconds or [])),
                    end_seconds=list(map(int, vector_metadata.position_end_seconds or [])),
                ),
            )


class IndexHostNotFound(Exception): ...


class VectorMetadata(BaseModel):
    """
    This class models what we index at Pinecone's metadata attribute for each vector.
    https://docs.pinecone.io/guides/data/filter-with-metadata
    """

    # Id filtering
    rid: str
    field_type: str
    field_id: str

    # Date range filtering
    date_created: Optional[int] = None
    date_modified: Optional[int] = None

    # Label filtering
    paragraph_labels: Optional[list[str]] = None
    field_labels: Optional[list[str]] = None

    # Security
    security_public: bool = True
    security_ids_with_access: Optional[list[str]] = None

    # Position
    position_start_seconds: Optional[list[str]] = None
    position_end_seconds: Optional[list[str]] = None
    page_number: Optional[int] = None

    # AI-tables metadata
    page_with_visual: Optional[bool] = None
    is_a_table: Optional[bool] = None
    representation_file: Optional[str] = None


class PineconeIndexManager(ExternalIndexManager):
    type = ExternalIndexProviderType.PINECONE
    supports_rollover = True

    def __init__(
        self,
        kbid: str,
        api_key: str,
        indexes: dict[str, kb_pb2.PineconeIndexMetadata],
        upsert_parallelism: int = 3,
        delete_parallelism: int = 2,
        upsert_timeout: float = 10.0,
        delete_timeout: float = 10.0,
        query_timeout: float = 10.0,
        default_vectorset: Optional[str] = None,
        rollover_indexes: Optional[dict[str, kb_pb2.PineconeIndexMetadata]] = None,
    ):
        super().__init__(kbid=kbid)
        assert api_key != ""
        self.api_key = api_key
        self.indexes = indexes
        self.rollover_indexes = rollover_indexes or {}
        self.pinecone = get_pinecone()
        self.upsert_parallelism = upsert_parallelism
        self.delete_parallelism = delete_parallelism
        self.upsert_timeout = upsert_timeout
        self.delete_timeout = delete_timeout
        self.query_timeout = query_timeout
        self.default_vectorset = default_vectorset

    def get_data_plane(self, index_host: str) -> DataPlane:
        return self.pinecone.data_plane(api_key=self.api_key, index_host=index_host)

    @classmethod
    async def create_indexes(
        cls,
        kbid: str,
        request: kb_pb2.CreateExternalIndexProviderMetadata,
        indexes: list[VectorsetExternalIndex],
    ) -> kb_pb2.StoredExternalIndexProviderMetadata:
        created_indexes = []
        metadata = kb_pb2.StoredExternalIndexProviderMetadata(
            type=kb_pb2.ExternalIndexProviderType.PINECONE
        )
        api_key = request.pinecone_config.api_key
        metadata.pinecone_config.encrypted_api_key = get_endecryptor().encrypt(api_key)
        metadata.pinecone_config.serverless_cloud = request.pinecone_config.serverless_cloud
        pinecone = get_pinecone().control_plane(api_key=api_key)
        serverless_cloud = to_pinecone_serverless_cloud_payload(request.pinecone_config.serverless_cloud)
        for index in indexes:
            vectorset_id = index.vectorset_id
            index_name = PineconeIndexManager.get_index_name()
            index_dimension = index.dimension
            similarity_metric = to_pinecone_index_metric(index.similarity)
            logger.info(
                "Creating pincone index",
                extra={
                    "kbid": kbid,
                    "index_name": index_name,
                    "similarity": similarity_metric,
                    "vector_dimension": index_dimension,
                    "vectorset_id": vectorset_id,
                    "cloud": serverless_cloud,
                },
            )
            try:
                index_host = await pinecone.create_index(
                    name=index_name,
                    dimension=index_dimension,
                    metric=similarity_metric,
                    serverless_cloud=serverless_cloud,
                )
                created_indexes.append(index_name)
            except PineconeAPIError as exc:
                # Try index creation rollback
                for index_name in created_indexes:
                    try:
                        await cls._delete_index(api_key, index_name)
                    except Exception:
                        logger.exception("Could not rollback created pinecone indexes")
                raise ExternalIndexCreationError("pinecone", exc.message) from exc
            metadata.pinecone_config.indexes[vectorset_id].CopyFrom(
                kb_pb2.PineconeIndexMetadata(
                    index_name=index_name,
                    index_host=index_host,
                    vector_dimension=index.dimension,
                    similarity=index.similarity,
                )
            )
        return metadata

    @classmethod
    async def delete_indexes(
        cls,
        kbid: str,
        stored: kb_pb2.StoredExternalIndexProviderMetadata,
    ) -> None:
        api_key = get_endecryptor().decrypt(stored.pinecone_config.encrypted_api_key)
        # Delete all indexes stored in the config and passed as parameters
        for index_metadata in stored.pinecone_config.indexes.values():
            index_name = index_metadata.index_name
            try:
                logger.info("Deleting pincone index", extra={"kbid": kbid, "index_name": index_name})
                await cls._delete_index(api_key, index_name)
            except Exception:
                logger.exception(
                    "Error deleting pinecone index", extra={"kbid": kbid, "index_name": index_name}
                )

    @classmethod
    @backoff.on_exception(
        backoff.expo,
        (PineconeAPIError,),
        jitter=backoff.random_jitter,
        max_tries=3,
    )
    async def _delete_index(cls, api_key: str, index_name: str) -> None:
        control_plane = get_pinecone().control_plane(api_key=api_key)
        await control_plane.delete_index(index_name)

    async def rollover_create_indexes(
        self, stored: kb_pb2.StoredExternalIndexProviderMetadata
    ) -> kb_pb2.StoredExternalIndexProviderMetadata:
        result = kb_pb2.StoredExternalIndexProviderMetadata()
        result.CopyFrom(stored)
        control_plane = get_pinecone().control_plane(api_key=self.api_key)
        created_indexes = []
        cloud = to_pinecone_serverless_cloud_payload(stored.pinecone_config.serverless_cloud)
        try:
            for vectorset_id, index in stored.pinecone_config.indexes.items():
                rollover_index_name = PineconeIndexManager.get_index_name()
                index_dimension = index.vector_dimension
                similarity_metric = to_pinecone_index_metric(index.similarity)
                logger.info(
                    "Creating pincone rollover index",
                    extra={
                        "kbid": self.kbid,
                        "index_name": index.index_name,
                        "rollover_index_name": rollover_index_name,
                        "similarity": similarity_metric,
                        "vector_dimension": index_dimension,
                        "vectorset_id": vectorset_id,
                    },
                )
                try:
                    index_host = await control_plane.create_index(
                        name=rollover_index_name,
                        dimension=index_dimension,
                        metric=similarity_metric,
                        serverless_cloud=cloud,
                    )
                    result.pinecone_config.indexes[vectorset_id].MergeFrom(
                        kb_pb2.PineconeIndexMetadata(
                            index_name=rollover_index_name,
                            index_host=index_host,
                            vector_dimension=index_dimension,
                            similarity=index.similarity,
                        )
                    )
                    created_indexes.append(rollover_index_name)
                except PineconeAPIError as exc:
                    raise ExternalIndexCreationError("pinecone", exc.message) from exc
        except Exception:
            # Rollback any created indexes
            for index_name in created_indexes:
                try:
                    await self.__class__._delete_index(self.api_key, index_name)
                except Exception:
                    logger.exception(
                        f"Could not rollback created pinecone index",
                        extra={
                            "kbid": self.kbid,
                            "index_name": index_name,
                        },
                    )
            raise

        # Wait for all indexes to be in the ready state
        wait_tasks = []
        for index_name in created_indexes:
            wait_tasks.append(
                asyncio.create_task(self.wait_for_index_ready(index_name, max_wait_seconds=60))
            )
        if len(wait_tasks) > 0:
            try:
                await asyncio.gather(*wait_tasks)
            except asyncio.TimeoutError:
                logger.warning(
                    "Timeout waiting for pinecone indexes to be ready",
                    extra={"kbid": self.kbid, "indexes": created_indexes},
                )

        # Clear the rollover indexes and update the stored metadata
        self.rollover_indexes.clear()
        self.rollover_indexes = dict(result.pinecone_config.indexes)
        return result

    async def wait_for_index_ready(self, index_name: str, max_wait_seconds: int = 10) -> None:
        """
        Wait for an index to be ready.
        Params:
        - `name`: The name of the index to wait for.
        - `max_wait_seconds`: The maximum number of seconds to wait.
        """
        control_plane = self.pinecone.control_plane(api_key=self.api_key)
        for _ in range(max_wait_seconds):
            try:
                index = await control_plane.describe_index(index_name)
                if index.status.ready:
                    return
            except PineconeAPIError:
                logger.exception(
                    "Failed to describe index while waiting for it to become ready.",
                    extra={"kbid": self.kbid, "index_name": index_name},
                )
            await asyncio.sleep(1)

        raise TimeoutError(f"Index {index_name} did not become ready after {max_wait_seconds} seconds.")

    async def rollover_cutover_indexes(self) -> None:
        assert len(self.rollover_indexes) > 0, "No rollover indexes to cutover to"
        control_plane = self.pinecone.control_plane(api_key=self.api_key)
        for index in self.indexes.values():
            index_name = index.index_name
            try:
                await control_plane.delete_index(index.index_name)
            except Exception:
                logger.exception(
                    "Error deleting pinecone index on cutover",
                    extra={"kbid": self.kbid, "index_name": index_name},
                )
        self.indexes.clear()
        self.indexes.update(self.rollover_indexes)

    @classmethod
    def get_index_name(cls) -> str:
        """
        Index names can't be longer than 45 characters and can only contain
        alphanumeric lowercase characters: https://docs.pinecone.io/troubleshooting/restrictions-on-index-names

        We generate a unique id for each pinecone index created.
        `nuclia-` is prepended to easily identify which indexes are created by Nuclia.

        Example:
        >>> get_index_name()
        'nuclia-2d899e8a0af54ac9a5addbd483d02ec9'
        """
        return f"nuclia-{uuid4().hex}"

    async def _delete_resource_to_index(self, index_host: str, resource_uuid: str) -> None:
        data_plane = self.get_data_plane(index_host=index_host)
        with manager_observer({"operation": "delete_by_resource_prefix"}):
            await data_plane.delete_by_id_prefix(
                id_prefix=resource_uuid,
                max_parallel_batches=self.delete_parallelism,
                batch_timeout=self.delete_timeout,
            )

    async def _delete_resource(self, resource_uuid: str) -> None:
        """
        Deletes by resource uuid on all indexes in parallel.
        """
        delete_tasks = []
        for index in self.indexes.values():
            index_host = index.index_host
            delete_tasks.append(
                asyncio.create_task(
                    self._delete_resource_to_index(
                        index_host=index_host,
                        resource_uuid=resource_uuid,
                    )
                )
            )
        if len(delete_tasks) > 0:
            await asyncio.gather(*delete_tasks)

    def get_index_host(self, vectorset_id: str, rollover: bool = False) -> str:
        if rollover:
            return self.rollover_indexes[vectorset_id].index_host
        else:
            return self.indexes[vectorset_id].index_host

    def get_prefixes_to_delete(self, index_data: Resource) -> dict[str, set[str]]:
        return {
            vectorset_id: set(prefixes_list.items)
            for vectorset_id, prefixes_list in index_data.vector_prefixes_to_delete.items()
        }

    async def _index_resource(
        self, resource_uuid: str, index_data: Resource, to_rollover_indexes: bool = False
    ) -> None:
        """
        Index NucliaDB resource into a Pinecone index.
        Handles multiple vectorsets.

        The algorithm is as follows:
        - First, get the vectorsets for which we have vectors to upsert.
        - Then, delete any previously existing vectors with the same field prefixes on all vectorsets.
        - Then, iterate the fields and the paragraphs to compute the base metadata for each vector.
        - After that, iterate the sentences now, and compute the list of vectors to upsert, and extend the vector
          metadata with any specific sentence metadata. This is done for each vectorset.
        - Finally, upsert the vectors to each vectorset index in parallel.
        """
        delete_tasks = []
        for vectorset, prefixes_to_delete in self.get_prefixes_to_delete(index_data).items():
            index_host = self.get_index_host(vectorset_id=vectorset, rollover=to_rollover_indexes)
            delete_tasks.append(
                asyncio.create_task(
                    self._delete_by_prefix_to_index(
                        index_host=index_host,
                        prefixes_to_delete=prefixes_to_delete,
                    )
                )
            )
        if len(delete_tasks) > 0:
            await asyncio.gather(*delete_tasks)

        with manager_observer({"operation": "compute_base_vector_metadatas"}):
            base_vector_metadatas: dict[str, VectorMetadata] = await self.compute_base_vector_metadatas(
                index_data, resource_uuid
            )

        with manager_observer({"operation": "compute_vectorset_vectors"}):
            vectorset_vectors: dict[str, list[PineconeVector]] = await self.compute_vectorset_vectors(
                index_data, base_vector_metadatas
            )

        upsert_tasks = []
        for vectorset_id, vectors in vectorset_vectors.items():
            index_host = self.get_index_host(vectorset_id=vectorset_id, rollover=to_rollover_indexes)
            upsert_tasks.append(
                asyncio.create_task(
                    self._upsert_to_index(
                        index_host=index_host,
                        vectors=vectors,
                    )
                )
            )
        if len(upsert_tasks) > 0:
            await asyncio.gather(*upsert_tasks)

    async def _upsert_to_index(self, index_host: str, vectors: list[PineconeVector]) -> None:
        if len(vectors) == 0:  # pragma: no cover
            return
        data_plane = self.get_data_plane(index_host=index_host)
        with manager_observer({"operation": "upsert_in_batches"}):
            await data_plane.upsert_in_batches(
                vectors=vectors,
                max_parallel_batches=self.upsert_parallelism,
                batch_timeout=self.upsert_timeout,
            )

    async def _delete_by_prefix_to_index(self, index_host: str, prefixes_to_delete: set[str]) -> None:
        if len(prefixes_to_delete) == 0:  # pragma: no cover
            return
        data_plane = self.get_data_plane(index_host=index_host)
        with manager_observer({"operation": "delete_by_prefix"}):
            for prefix in prefixes_to_delete:
                await data_plane.delete_by_id_prefix(
                    id_prefix=prefix,
                    max_parallel_batches=self.delete_parallelism,
                    batch_timeout=self.delete_timeout,
                )

    async def compute_base_vector_metadatas(
        self, index_data: Resource, resource_uuid: str
    ) -> dict[str, VectorMetadata]:
        # This is a CPU bound operation and when the number of vectors is large, it can take a
        # long time (around a second).
        # Ideally, we would use a ProcessPoolExecutor to parallelize the computation of the metadata, but
        # the Resource protobuf is not pickleable, so we can't use it in a ProcessPoolExecutor. This will
        # be less of a problem when we move pinecone indexing to its own consumer.
        return await asyncio.to_thread(self._compute_base_vector_metadatas, index_data, resource_uuid)

    def _compute_base_vector_metadatas(
        self, index_data: Resource, resource_uuid: str
    ) -> dict[str, VectorMetadata]:
        """
        Compute the base metadata for each vector in the resource.
        This metadata is common to all vectors in the same paragraph, for all vectorsets.
        """
        metadatas: dict[str, VectorMetadata] = {}
        security_public = True
        security_ids_with_access = None
        if index_data.HasField("security"):
            security_public = False
            security_ids_with_access = list(set(index_data.security.access_groups))

        resource_labels = set(index_data.labels)
        date_created = index_data.metadata.created.ToSeconds()
        date_modified = index_data.metadata.modified.ToSeconds()

        # First off, iterate the fields and the paragraphs to compute the metadata for
        # each vector, specifically the labels that will be used for filtering.
        for field_id, text_info in index_data.texts.items():
            field_labels = set(text_info.labels)
            field_paragraphs = index_data.paragraphs.get(field_id)
            if field_paragraphs is None:
                logger.info(
                    "Paragraphs not found for field",
                    extra={"kbid": self.kbid, "rid": resource_uuid, "field_id": field_id},
                )
                continue

            paragraph: IndexParagraph
            for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
                fid = ParagraphId.from_string(paragraph_id).field_id
                vector_metadata = VectorMetadata(
                    rid=resource_uuid,
                    field_type=fid.type,
                    field_id=fid.key,
                    date_created=date_created,
                    date_modified=date_modified,
                    security_public=security_public,
                    security_ids_with_access=security_ids_with_access,
                )
                metadatas[paragraph_id] = vector_metadata
                final_field_labels = resource_labels.union(field_labels)
                if final_field_labels:
                    vector_metadata.field_labels = unique(discard_labels(list(final_field_labels)))
                final_paragraph_labels = paragraph.labels
                if final_paragraph_labels:
                    vector_metadata.paragraph_labels = unique(
                        discard_labels(list(final_paragraph_labels))
                    )
        return metadatas

    async def compute_vectorset_vectors(
        self, index_data: Resource, base_vector_metadatas: dict[str, VectorMetadata]
    ) -> dict[str, list[PineconeVector]]:
        # This is a CPU bound operation and when the number of vectors is large, it can take a
        # long time (around a second).
        # Ideally, we would use a ProcessPoolExecutor to parallelize the computation of the metadata, but
        # the Resource protobuf is not pickleable, so we can't use it in a ProcessPoolExecutor. This will
        # be less of a problem when we move pinecone indexing to its own consumer.
        return await asyncio.to_thread(
            self._compute_vectorset_vectors, index_data, base_vector_metadatas
        )

    def _compute_vectorset_vectors(
        self, index_data: Resource, base_vector_metadatas: dict[str, VectorMetadata]
    ) -> dict[str, list[PineconeVector]]:
        vectorset_vectors: dict[str, list[PineconeVector]] = {}
        for index_paragraph_id, index_paragraph in iter_paragraphs(index_data):
            # We must compute the vectors for each vectorset present the paragraph.
            vectorset_iterators = {}
            if index_paragraph.sentences and self.default_vectorset:
                vectorset_iterators[self.default_vectorset] = index_paragraph.sentences.items()
            for vectorset_id, vector_sentences in index_paragraph.vectorsets_sentences.items():
                if vector_sentences.sentences:
                    vectorset_iterators[vectorset_id] = vector_sentences.sentences.items()

            vector_sentence: VectorSentence
            for vectorset_id, sentences_iterator in vectorset_iterators.items():
                for sentence_id, vector_sentence in sentences_iterator:
                    vector_metadata_to_copy = base_vector_metadatas.get(index_paragraph_id)
                    if vector_metadata_to_copy is None:
                        logger.warning(
                            f"Metadata not found for sentences of paragraph {index_paragraph_id}"
                        )
                        continue
                    # Copy the initial metadata collected at paragraph parsing in case
                    # the metadata is different for each vectorset
                    vector_metadata = deepcopy(vector_metadata_to_copy)

                    # AI-tables metadata
                    if vector_sentence.metadata.page_with_visual:
                        vector_metadata.page_with_visual = True
                    if vector_sentence.metadata.representation.is_a_table:
                        vector_metadata.is_a_table = True
                    if vector_sentence.metadata.representation.file:
                        vector_metadata.representation_file = (
                            vector_sentence.metadata.representation.file
                        )

                    # Video positions
                    if len(vector_sentence.metadata.position.start_seconds):
                        vector_metadata.position_start_seconds = list(
                            map(str, vector_sentence.metadata.position.start_seconds)
                        )
                    if len(vector_sentence.metadata.position.end_seconds):
                        vector_metadata.position_end_seconds = list(
                            map(str, vector_sentence.metadata.position.end_seconds)
                        )
                    vector_metadata.page_number = vector_sentence.metadata.position.page_number
                    try:
                        pc_vector = PineconeVector(
                            id=sentence_id,
                            values=list(vector_sentence.vector),
                            metadata=vector_metadata.model_dump(exclude_none=True),
                        )
                    except MetadataTooLargeError as exc:  # pragma: no cover
                        logger.error(f"Invalid Pinecone vector. Metadata is too large. Skipping: {exc}")
                        continue

                    vectors = vectorset_vectors.setdefault(vectorset_id, [])
                    vectors.append(pc_vector)
        return vectorset_vectors

    async def _query(self, request: SearchRequest) -> PineconeQueryResults:
        if len(request.vector) == 0:
            return PineconeQueryResults(results=QueryResponse(matches=[]))
        vectorset_id = request.vectorset or self.default_vectorset or "__default__"
        index_host = self.get_index_host(vectorset_id=vectorset_id)
        data_plane = self.get_data_plane(index_host=index_host)
        filter = convert_to_pinecone_filter(request)
        top_k = request.result_per_page
        query_results = await data_plane.query(
            vector=list(request.vector),
            top_k=top_k,
            include_values=False,
            include_metadata=True,
            filter=filter,
            timeout=self.query_timeout,
        )
        # filter by min score manually, as Pinecone don't implement this feature
        results = QueryResponse(
            matches=[
                match for match in query_results.matches if match.score >= request.min_score_semantic
            ]
        )
        return PineconeQueryResults(results=results)

    async def _get_index_counts(self) -> IndexCounts:
        if self.kbid in COUNTERS_CACHE:
            # Cache hit
            return COUNTERS_CACHE[self.kbid]
        total = IndexCounts(fields=0, paragraphs=0, sentences=0, size_bytes=0)
        tasks = []
        vectorset_results: dict[str, IndexCounts] = {}

        for vectorset_id in self.indexes.keys():
            tasks.append(
                asyncio.create_task(self._get_vectorset_index_counts(vectorset_id, vectorset_results))
            )
        if len(tasks) > 0:
            await asyncio.gather(*tasks)

        for _, counts in vectorset_results.items():
            total.paragraphs += counts.paragraphs
            total.sentences += counts.sentences
        COUNTERS_CACHE[self.kbid] = total
        return total

    async def _get_vectorset_index_counts(
        self, vectorset_id: str, results: dict[str, IndexCounts]
    ) -> None:
        index_host = self.get_index_host(vectorset_id=vectorset_id)
        data_plane = self.get_data_plane(index_host=index_host)
        try:
            index_stats = await data_plane.stats()
            results[vectorset_id] = IndexCounts(
                fields=0,
                paragraphs=index_stats.totalVectorCount,
                sentences=index_stats.totalVectorCount,
                size_bytes=0,
            )
        except Exception:
            logger.exception(
                "Error getting index stats",
                extra={"kbid": self.kbid, "provider": self.type.value, "index_host": index_host},
            )


def discard_labels(labels: list[str]) -> list[str]:
    return [
        label
        for label in labels
        if not any(label.startswith(prefix) for prefix in DISCARDED_LABEL_PREFIXES)
    ]


def unique(labels: list[str]) -> list[str]:
    return list(set(labels))


def convert_to_pinecone_filter(request: SearchRequest) -> Optional[dict[str, Any]]:
    """
    Returns a Pinecone filter from a SearchRequest so that RAG features supported by Nuclia
    can be used on Pinecone indexes.
    """
    and_terms = []
    if request.HasField("field_filter"):
        and_terms.append(convert_filter_expression("field_labels", request.field_filter))
    if request.HasField("paragraph_filter"):
        and_terms.append(convert_filter_expression("paragraph_labels", request.field_filter))

    if len(request.security.access_groups):
        # Security filtering
        security_term = {
            LogicalOperator.OR: [
                {"security_public": {"$eq": True}},
                {
                    "security_ids_with_access": {
                        FilterOperator.IN: list(set(request.security.access_groups))
                    }
                },
            ]
        }
        and_terms.append(security_term)

    if len(and_terms) == 0:
        return None
    if len(and_terms) == 1:
        return and_terms[0]
    return {LogicalOperator.AND: and_terms}


def convert_filter_expression(
    field: str, expression: FilterExpression, negative: bool = False
) -> dict[str, Any]:
    """
    Converts internal label filter expressions to Pinecone's metadata query language.

    Note: Since Pinecone does not support negation of expressions, we need to use De Morgan's laws to
    convert the expression to a positive one.
    """

    kind = expression.WhichOneof("expr")
    if kind == "bool_and":
        if negative:
            return {
                LogicalOperator.OR: [
                    convert_filter_expression(field, sub_expression, negative=True)
                    for sub_expression in expression.bool_and.operands
                ]
            }
        else:
            return {
                LogicalOperator.AND: [
                    convert_filter_expression(field, sub_expression)
                    for sub_expression in expression.bool_and.operands
                ]
            }
    elif kind == "bool_or":
        if negative:
            return {
                LogicalOperator.AND: [
                    convert_filter_expression(field, sub_expression, negative=True)
                    for sub_expression in expression.bool_or.operands
                ]
            }
        else:
            return {
                LogicalOperator.OR: [
                    convert_filter_expression(field, sub_expression)
                    for sub_expression in expression.bool_or.operands
                ]
            }

    elif kind == "bool_not":
        return convert_filter_expression(field, expression.bool_not, negative=not negative)

    elif kind == "resource":
        operator = FilterOperator.NOT_EQUALS if negative else FilterOperator.EQUALS
        return {"rid": {operator: expression.resource.resource_id}}

    elif kind == "field":
        field_id = expression.field.field_type
        if expression.field.HasField("field_id"):
            field_id += f"/{expression.field.field_id}"
        operator = FilterOperator.NOT_EQUALS if negative else FilterOperator.EQUALS
        return {"field_id": {operator: field_id}}

    elif kind == "keyword":
        raise ValueError("Cannot filter by keywords")

    elif kind == "date":
        date_field = (
            "date_created"
            if expression.date.field == FilterExpression.DateRangeFilter.DateField.CREATED
            else "date_modified"
        )
        if negative:
            terms = []
            if expression.date.HasField("since"):
                operator = FilterOperator.LESS_THAN
                terms.append({date_field: {operator: expression.date.since.ToSeconds()}})
            if expression.date.HasField("until"):
                operator = FilterOperator.GREATER_THAN
                terms.append({date_field: {operator: expression.date.until.ToSeconds()}})

            if len(terms) == 2:
                return {LogicalOperator.OR: terms}
            elif len(terms) == 1:
                return terms[0]
            else:
                raise ValueError(f"Invalid filter expression: {expression}")
        else:
            terms = []
            if expression.date.HasField("since"):
                operator = FilterOperator.GREATER_THAN_OR_EQUAL
                terms.append({date_field: {operator: expression.date.since.ToSeconds()}})
            if expression.date.HasField("until"):
                operator = FilterOperator.LESS_THAN_OR_EQUAL
                terms.append({date_field: {operator: expression.date.until.ToSeconds()}})

            if len(terms) == 2:
                return {LogicalOperator.AND: terms}
            elif len(terms) == 1:
                return terms[0]
            else:
                raise ValueError(f"Invalid filter expression: {expression}")

    elif kind == "facet":
        operator = FilterOperator.NOT_IN if negative else FilterOperator.IN
        return {field: {operator: [expression.facet.facet]}}

    else:
        raise ValueError(f"Invalid filter expression: {expression}")


def iter_paragraphs(resource: Resource) -> Iterator[tuple[str, IndexParagraph]]:
    for _, paragraphs in resource.paragraphs.items():
        for paragraph_id, paragraph in paragraphs.paragraphs.items():
            yield paragraph_id, paragraph


def to_pinecone_index_metric(similarity: utils_pb2.VectorSimilarity.ValueType) -> str:
    return {
        utils_pb2.VectorSimilarity.COSINE: "cosine",
        utils_pb2.VectorSimilarity.DOT: "dotproduct",
    }[similarity]


def to_pinecone_serverless_cloud_payload(
    serverless: kb_pb2.PineconeServerlessCloud.ValueType,
) -> dict[str, str]:
    return {
        kb_pb2.PineconeServerlessCloud.AWS_EU_WEST_1: {
            "cloud": "aws",
            "region": "eu-west-1",
        },
        kb_pb2.PineconeServerlessCloud.AWS_US_EAST_1: {
            "cloud": "aws",
            "region": "us-east-1",
        },
        kb_pb2.PineconeServerlessCloud.AWS_US_WEST_2: {
            "cloud": "aws",
            "region": "us-west-2",
        },
        kb_pb2.PineconeServerlessCloud.AZURE_EASTUS2: {
            "cloud": "azure",
            "region": "eastus2",
        },
        kb_pb2.PineconeServerlessCloud.GCP_US_CENTRAL1: {
            "cloud": "gcp",
            "region": "us-central1",
        },
    }[serverless]
