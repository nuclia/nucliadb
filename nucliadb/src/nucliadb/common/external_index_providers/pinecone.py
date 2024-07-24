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
import json
import logging
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
from nucliadb_utils.aiopynecone.client import FilterOperator, LogicalOperator
from nucliadb_utils.aiopynecone.exceptions import MetadataTooLargeError
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
        for sentence_id in index_data.sentences_to_delete:
            try:
                delete_field = FieldId.from_string(sentence_id)
                field_prefixes_to_delete.add(delete_field.full())
            except ValueError:  # pragma: no cover
                logger.warning(f"Invalid id to delete: {sentence_id}. VectorId expected.")
                continue
        for paragraph_id in index_data.paragraphs_to_delete:
            try:
                delete_pid = ParagraphId.from_string(paragraph_id)
                field_prefixes_to_delete.add(delete_pid.field_id.full())
            except ValueError:  # pragma: no cover
                try:
                    delete_field = FieldId.from_string(paragraph_id)
                    field_prefixes_to_delete.add(delete_field.full())
                except ValueError:
                    logger.warning(f"Invalid id to delete: {paragraph_id}. ParagraphId expected.")
                    continue

        with manager_observer({"operation": "delete_by_field_prefix"}):
            for prefix in field_prefixes_to_delete:
                await self.data_plane.delete_by_id_prefix(
                    id_prefix=prefix,
                    max_parallel_batches=self.delete_parallelism,
                    batch_timeout=self.delete_timeout,
                )

        security_public = True
        security_ids_with_access = None
        if index_data.HasField("security"):
            security_public = False
            security_ids_with_access = list(set(index_data.security.access_groups))

        resource_labels = set(index_data.labels)
        date_created = index_data.metadata.created.ToSeconds()
        date_modified = index_data.metadata.modified.ToSeconds()

        metadatas: dict[str, VectorMetadata] = {}
        metadata: Optional[VectorMetadata]

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
                metadatas[paragraph_id] = VectorMetadata(
                    rid=resource_uuid,
                    field_type=fid.field_type,
                    field_id=fid.field_id,
                    field_labels=list(resource_labels.union(field_labels)),
                    paragraph_labels=list(paragraph.labels),
                    date_created=date_created,
                    date_modified=date_modified,
                    security_public=security_public,
                    security_ids_with_access=security_ids_with_access,
                )

        # Then iterate the sentences now, and compute the list of vectors to upsert, along with their metadata.
        vectors: list[PineconeVector] = []
        for _, index_paragraphs in index_data.paragraphs.items():
            for index_paragraph_id, index_paragraph in index_paragraphs.paragraphs.items():
                metadata = metadatas.get(index_paragraph_id)
                if metadata is None:  # pragma: no cover
                    logger.warning(f"Metadata not found for paragraph {index_paragraph_id}")
                    continue

                vector_sentence: VectorSentence
                for sentence_id, vector_sentence in index_paragraph.sentences.items():
                    metadata = metadatas.get(index_paragraph_id)
                    if metadata is None:
                        logger.warning(
                            f"Metadata not found for sentences of paragraph {index_paragraph_id}"
                        )
                        continue

                    # Empty lists are not supported in Pinecone metadata
                    if metadata.paragraph_labels is not None:
                        if len(metadata.paragraph_labels) == 0:
                            metadata.paragraph_labels = None
                        else:
                            metadata.paragraph_labels = unique(discard_labels(metadata.paragraph_labels))
                    if metadata.field_labels is not None:
                        if len(metadata.field_labels) == 0:
                            metadata.field_labels = None
                        else:
                            metadata.field_labels = unique(discard_labels(metadata.field_labels))

                    # AI-tables metadata
                    if vector_sentence.metadata.page_with_visual:
                        metadata.page_with_visual = True
                    if vector_sentence.metadata.representation.is_a_table:
                        metadata.is_a_table = True
                    if vector_sentence.metadata.representation.file:
                        metadata.representation_file = vector_sentence.metadata.representation.file

                    # Video positions
                    if len(vector_sentence.metadata.position.start_seconds):
                        metadata.position_start_seconds = list(
                            map(str, vector_sentence.metadata.position.start_seconds)
                        )
                    if len(vector_sentence.metadata.position.end_seconds):
                        metadata.position_end_seconds = list(
                            map(str, vector_sentence.metadata.position.end_seconds)
                        )
                    metadata.page_number = vector_sentence.metadata.position.page_number
                    try:
                        pc_vector = PineconeVector(
                            id=sentence_id,
                            values=list(vector_sentence.vector),
                            metadata=metadata.model_dump(exclude_none=True),
                        )
                    except MetadataTooLargeError as exc:  # pragma: no cover
                        logger.error(f"Invalid Pinecone vector. Metadata is too large. Skipping: {exc}")
                        continue
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
        filter = convert_to_pinecone_filter(request)
        top_k = (request.page_number + 1) * request.result_per_page
        query_results = await self.data_plane.query(
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
    and_terms = []
    if request.HasField("filter"):
        if len(request.filter.paragraph_labels) > 0 and len(request.filter.field_labels) > 0:
            raise ValueError("Cannot filter by paragraph and field labels at the same request")

        decoded_expression: dict[str, Any] = json.loads(request.filter.expression)
        if len(request.filter.paragraph_labels) > 0:
            and_terms.append(convert_label_filter_expression("paragraph_labels", decoded_expression))
        else:
            and_terms.append(convert_label_filter_expression("field_labels", decoded_expression))
    if request.HasField("timestamps"):
        and_terms.extend(convert_timestamp_filter(request.timestamps))

    if len(request.key_filters) > 0:
        and_terms.append({"rid": {FilterOperator.IN: list(set(request.key_filters))}})

    if len(request.security.access_groups):
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
    return {LogicalOperator.AND: and_terms}


def convert_label_filter_expression(
    field: str, expression: dict[str, Any], negative: bool = False
) -> dict[str, Any]:
    """
    Converts internal label filter expressions to Pinecone's metadata query language.
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
