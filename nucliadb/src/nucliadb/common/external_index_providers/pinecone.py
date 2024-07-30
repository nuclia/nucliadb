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
import json
import logging
from copy import deepcopy
from typing import Any, Iterator, Optional

from pydantic import BaseModel

from nucliadb.common.external_index_providers.base import (
    ExternalIndexManager,
    ExternalIndexProviderType,
    QueryResults,
    TextBlockMatch,
)
from nucliadb.common.ids import FieldId, ParagraphId, VectorId
from nucliadb_protos.nodereader_pb2 import SearchRequest, Timestamps
from nucliadb_protos.noderesources_pb2 import IndexParagraph, Resource, VectorSentence
from nucliadb_telemetry.metrics import Observer
from nucliadb_utils.aiopynecone.client import DataPlane, FilterOperator, LogicalOperator
from nucliadb_utils.aiopynecone.exceptions import MetadataTooLargeError
from nucliadb_utils.aiopynecone.models import MAX_INDEX_NAME_LENGTH, QueryResponse
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
            except ValueError:  # pragma: no cover
                logger.error(f"Invalid Pinecone vector id: {matching_vector.id}")
                continue
            vector_metadata = VectorMetadata.model_validate(matching_vector.metadata)  # noqa
            yield TextBlockMatch(
                text=None,  # To be filled by the results hydrator
                id=matching_vector.id,
                resource_id=vector_id.field_id.rid,
                field_id=vector_id.field_id.full(),
                score=matching_vector.score,
                order=order,
                position_start=vector_id.vector_start,
                position_end=vector_id.vector_end,
                subfield_id=vector_id.field_id.subfield_id,
                index=vector_id.index,
                position_start_seconds=list(map(int, vector_metadata.position_start_seconds or [])),
                position_end_seconds=list(map(int, vector_metadata.position_end_seconds or [])),
                is_a_table=vector_metadata.is_a_table or False,
                page_with_visual=vector_metadata.page_with_visual or False,
                representation_file=vector_metadata.representation_file,
                paragraph_labels=vector_metadata.paragraph_labels or [],
                field_labels=vector_metadata.field_labels or [],
                page_number=vector_metadata.page_number,
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

    def __init__(
        self,
        kbid: str,
        api_key: str,
        index_hosts: dict[str, str],
        upsert_parallelism: int = 3,
        delete_parallelism: int = 2,
        upsert_timeout: float = 10.0,
        delete_timeout: float = 10.0,
        query_timeout: float = 10.0,
    ):
        super().__init__(kbid=kbid)
        assert api_key != ""
        self.api_key = api_key
        assert index_hosts != {}
        self.index_hosts = index_hosts
        self.pinecone = get_pinecone()
        self.upsert_parallelism = upsert_parallelism
        self.delete_parallelism = delete_parallelism
        self.upsert_timeout = upsert_timeout
        self.delete_timeout = delete_timeout
        self.query_timeout = query_timeout

    def get_data_plane(self, index_name: str) -> DataPlane:
        try:
            index_host = self.index_hosts[index_name]
        except KeyError:
            raise IndexHostNotFound(index_name)
        return self.pinecone.data_plane(api_key=self.api_key, index_host=index_host)

    @classmethod
    def get_index_name(cls, kbid: str, vectorset_id: str) -> str:
        """
        Index names can't be longer than 45 characters and can only contain
        alphanumeric lowercase characters: https://docs.pinecone.io/troubleshooting/restrictions-on-index-names

        We need to include both the kbid and the vectorset id in the index name,
        so we don't create two conflicting Pinecone indexes for the same api_key.

        Take the two left-most parts from the kbid and prepend it the vectorset_id.
        Example:
        >>> get_index_name('7b7887b4-2d78-41c7-a398-586af7d7db8b', 'multilingual-2024-05-08')
        'multilingual-2024-05-08--7b7887b4-2d78'
        """
        kbid_part = "-".join(kbid.split("-")[:2])
        index_name = f"{vectorset_id}--{kbid_part}"
        index_name = index_name.lower()
        index_name = index_name.replace("_", "-")
        if len(index_name) > MAX_INDEX_NAME_LENGTH:
            raise ValueError("Index name is too large!")
        return index_name

    async def _delete_resource_to_index(self, index_name: str, resource_uuid: str) -> None:
        data_plane = self.get_data_plane(index_name)
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
        for index_name in self.index_hosts:
            delete_tasks.append(
                asyncio.create_task(
                    self._delete_resource_to_index(
                        index_name=index_name,
                        resource_uuid=resource_uuid,
                    )
                )
            )
        if len(delete_tasks) > 0:
            await asyncio.gather(*delete_tasks)

    def get_vectorsets_in_resource(self, index_data: Resource) -> set[str]:
        vectorsets: set[str] = set()
        for _, paragraphs in index_data.paragraphs.items():
            for _, paragraph in paragraphs.paragraphs.items():
                if paragraph.sentences:
                    vectorsets.add("default")
                for vectorset_id, vectorsets_sentences in paragraph.vectorsets_sentences.items():
                    if vectorsets_sentences.sentences:
                        vectorsets.add(vectorset_id)
        return vectorsets

    def get_prefixes_to_delete(self, index_data: Resource) -> set[str]:
        prefixes_to_delete = set()
        for sentence_id in index_data.sentences_to_delete:
            try:
                delete_field = FieldId.from_string(sentence_id)
                prefixes_to_delete.add(delete_field.full())
            except ValueError:  # pragma: no cover
                logger.warning(f"Invalid id to delete: {sentence_id}. VectorId expected.")
                continue
        for paragraph_id in index_data.paragraphs_to_delete:
            try:
                delete_pid = ParagraphId.from_string(paragraph_id)
                prefixes_to_delete.add(delete_pid.field_id.full())
            except ValueError:  # pragma: no cover
                try:
                    delete_field = FieldId.from_string(paragraph_id)
                    prefixes_to_delete.add(delete_field.full())
                except ValueError:
                    logger.warning(f"Invalid id to delete: {paragraph_id}. ParagraphId expected.")
                    continue
        return prefixes_to_delete

    async def _index_resource(self, resource_uuid: str, index_data: Resource) -> None:
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
        vectorsets = self.get_vectorsets_in_resource(index_data)
        prefixes_to_delete = self.get_prefixes_to_delete(index_data)
        delete_tasks = []
        for vectorset in vectorsets:
            delete_tasks.append(
                asyncio.create_task(
                    self._delete_by_prefix_to_vectorset_index(
                        vectorset_id=vectorset,
                        prefixes_to_delete=prefixes_to_delete,
                    )
                )
            )
        if len(delete_tasks) > 0:
            await asyncio.gather(*delete_tasks)

        with manager_observer({"operation": "compute_base_vector_metadatas"}):
            base_vector_metadatas: dict[str, VectorMetadata] = self.compute_base_vector_metadatas(
                index_data, resource_uuid
            )

        with manager_observer({"operation": "compute_vectorset_vectors"}):
            vectorset_vectors: dict[str, list[PineconeVector]] = self.compute_vectorset_vectors(
                index_data, base_vector_metadatas
            )

        upsert_tasks = []
        for vectorset_id, vectors in vectorset_vectors.items():
            upsert_tasks.append(
                asyncio.create_task(
                    self._upsert_to_vectorset_index(
                        vectorset_id=vectorset_id,
                        vectors=vectors,
                    )
                )
            )
        if len(upsert_tasks) > 0:
            await asyncio.gather(*upsert_tasks)

    async def _upsert_to_vectorset_index(self, vectorset_id: str, vectors: list[PineconeVector]) -> None:
        if len(vectors) == 0:  # pragma: no cover
            return
        index_name = self.get_index_name(self.kbid, vectorset_id)
        try:
            data_plane = self.get_data_plane(index_name)
        except IndexHostNotFound:  # pragma: no cover
            logger.error(
                "Data to index for index which host could not be found",
                extra={
                    "kbid": self.kbid,
                    "provider": self.type.value,
                    "vectorset_id": vectorset_id,
                    "index_name": index_name,
                    "index_hosts": self.index_hosts,
                },
            )
            return
        with manager_observer({"operation": "upsert_in_batches"}):
            await data_plane.upsert_in_batches(
                vectors=vectors,
                max_parallel_batches=self.upsert_parallelism,
                batch_timeout=self.upsert_timeout,
            )

    async def _delete_by_prefix_to_vectorset_index(
        self, vectorset_id: str, prefixes_to_delete: set[str]
    ) -> None:
        if len(prefixes_to_delete) == 0:  # pragma: no cover
            return
        index_name = self.get_index_name(self.kbid, vectorset_id)
        data_plane = self.get_data_plane(index_name)
        with manager_observer({"operation": "delete_by_prefix"}):
            for prefix in prefixes_to_delete:
                await data_plane.delete_by_id_prefix(
                    id_prefix=prefix,
                    max_parallel_batches=self.delete_parallelism,
                    batch_timeout=self.delete_timeout,
                )

    def compute_base_vector_metadatas(
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
                logger.warning(f"Paragraphs not found for field {field_id}")
                continue

            paragraph: IndexParagraph
            for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
                fid = ParagraphId.from_string(paragraph_id).field_id
                vector_metadata = VectorMetadata(
                    rid=resource_uuid,
                    field_type=fid.field_type,
                    field_id=fid.field_id,
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

    def compute_vectorset_vectors(
        self, index_data: Resource, base_vector_metadatas: dict[str, VectorMetadata]
    ) -> dict[str, list[PineconeVector]]:
        vectorset_vectors: dict[str, list[PineconeVector]] = {}
        for _, index_paragraphs in index_data.paragraphs.items():
            for index_paragraph_id, index_paragraph in index_paragraphs.paragraphs.items():
                # We must compute the vectors for each vectorset present the paragraph.
                vectorset_iterators = []
                if index_paragraph.sentences:
                    vectorset_iterators.append(("default", index_paragraph.sentences.items()))
                for vectorset_id, vector_sentences in index_paragraph.vectorsets_sentences.items():
                    if vector_sentences.sentences:
                        vectorset_iterators.append((vectorset_id, vector_sentences.sentences.items()))

                vector_sentence: VectorSentence
                for vectorset_id, sentences_iterator in vectorset_iterators:
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
                            logger.error(
                                f"Invalid Pinecone vector. Metadata is too large. Skipping: {exc}"
                            )
                            continue

                        vectors = vectorset_vectors.setdefault(vectorset_id, [])
                        vectors.append(pc_vector)
        return vectorset_vectors

    async def _query(self, request: SearchRequest) -> PineconeQueryResults:
        vectorset_id = request.vectorset or "default"
        index_name = self.get_index_name(self.kbid, vectorset_id)
        data_plane = self.get_data_plane(index_name)
        filter = convert_to_pinecone_filter(request)
        top_k = (request.page_number + 1) * request.result_per_page
        query_results = await data_plane.query(
            vector=list(request.vector),
            top_k=top_k,
            include_values=False,
            include_metadata=True,
            filter=filter,
            timeout=self.query_timeout,
        )
        return PineconeQueryResults(results=query_results)


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
    if request.HasField("filter"):
        # Label filtering
        if len(request.filter.paragraph_labels) > 0 and len(request.filter.field_labels) > 0:
            raise ValueError("Cannot filter by paragraph and field labels at the same request")

        decoded_expression: dict[str, Any] = json.loads(request.filter.expression)
        if len(request.filter.paragraph_labels) > 0:
            and_terms.append(convert_label_filter_expression("paragraph_labels", decoded_expression))
        else:
            and_terms.append(convert_label_filter_expression("field_labels", decoded_expression))

    if request.HasField("timestamps"):
        # Date range filtering
        and_terms.extend(convert_timestamp_filter(request.timestamps))

    if len(request.key_filters) > 0:
        # Filter by resource_id
        and_terms.append({"rid": {FilterOperator.IN: list(set(request.key_filters))}})

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

    if len(request.fields) > 0:
        # Filter by field_id
        fields_term = {
            "field_id": {FilterOperator.IN: list({field_id.strip("/") for field_id in request.fields})}
        }
        and_terms.append(fields_term)
    if len(and_terms) == 0:
        return None
    if len(and_terms) == 1:
        return and_terms[0]
    return {LogicalOperator.AND: and_terms}


def convert_label_filter_expression(
    field: str, expression: dict[str, Any], negative: bool = False
) -> dict[str, Any]:
    """
    Converts internal label filter expressions to Pinecone's metadata query language.

    Note: Since Pinecone does not support negation of expressions, we need to use De Morgan's laws to
    convert the expression to a positive one.
    """
    if "literal" in expression:
        if negative:
            return {field: {FilterOperator.NOT_IN: [expression["literal"]]}}
        else:
            return {field: {FilterOperator.IN: [expression["literal"]]}}

    if "and" in expression:
        if negative:
            return {
                LogicalOperator.OR: [
                    convert_label_filter_expression(field, sub_expression, negative=True)
                    for sub_expression in expression["and"]
                ]
            }
        else:
            return {
                LogicalOperator.AND: [
                    convert_label_filter_expression(field, sub_expression)
                    for sub_expression in expression["and"]
                ]
            }

    if "or" in expression:
        if negative:
            return {
                LogicalOperator.AND: [
                    convert_label_filter_expression(field, sub_expression, negative=True)
                    for sub_expression in expression["or"]
                ]
            }
        else:
            return {
                LogicalOperator.OR: [
                    convert_label_filter_expression(field, sub_expression)
                    for sub_expression in expression["or"]
                ]
            }

    if "not" in expression:
        return convert_label_filter_expression(field, expression["not"], negative=True)

    raise ValueError(f"Invalid label filter expression: {expression}")


def convert_timestamp_filter(timestamps: Timestamps) -> list[dict[str, Any]]:
    """
    Allows to filter by date_created and date_modified fields in Pinecone.
    Powers date range filtering at NucliaDB.
    """
    and_terms = []
    if timestamps.HasField("from_modified"):
        and_terms.append(
            {
                "date_modified": {
                    FilterOperator.GREATER_THAN_OR_EQUAL: timestamps.from_modified.ToSeconds()
                }
            }
        )
    if timestamps.HasField("to_modified"):
        and_terms.append(
            {"date_modified": {FilterOperator.LESS_THAN_OR_EQUAL: timestamps.to_modified.ToSeconds()}}
        )
    if timestamps.HasField("from_created"):
        and_terms.append(
            {"date_created": {FilterOperator.GREATER_THAN_OR_EQUAL: timestamps.from_created.ToSeconds()}}
        )
    if timestamps.HasField("to_created"):
        and_terms.append(
            {"date_created": {FilterOperator.LESS_THAN_OR_EQUAL: timestamps.to_created.ToSeconds()}}
        )
    return and_terms
