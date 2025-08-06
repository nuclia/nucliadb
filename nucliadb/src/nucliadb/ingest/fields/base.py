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
from __future__ import annotations

import asyncio
import enum
import logging
from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Optional, Type, TypeVar

from google.protobuf.message import DecodeError, Message

from nucliadb.common import datamanagers
from nucliadb.ingest.fields.exceptions import InvalidFieldClass, InvalidPBClass
from nucliadb_protos.knowledgebox_pb2 import VectorSetConfig
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldAuthor,
    FieldComputedMetadata,
    FieldComputedMetadataWrapper,
    FieldQuestionAnswers,
    FieldQuestionAnswerWrapper,
    LargeComputedMetadata,
    LargeComputedMetadataWrapper,
    QuestionAnswers,
)
from nucliadb_protos.utils_pb2 import ExtractedText, VectorObject
from nucliadb_protos.writer_pb2 import Error, FieldStatus
from nucliadb_utils import const
from nucliadb_utils.storages.exceptions import CouldNotCopyNotFound
from nucliadb_utils.storages.storage import Storage, StorageField
from nucliadb_utils.utilities import has_feature

logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover
    from nucliadb.ingest.orm.resource import Resource


SUBFIELDFIELDS = ("c",)


# NOTE extracted vectors key is no longer a static key, it is stored in each
# vectorset
class FieldTypes(str, enum.Enum):
    FIELD_TEXT = "extracted_text"
    FIELD_VECTORS = "extracted_vectors"
    FIELD_VECTORSET = "{vectorset}/extracted_vectors"
    FIELD_METADATA = "metadata"
    FIELD_LARGE_METADATA = "large_metadata"
    THUMBNAIL = "thumbnail"
    QUESTION_ANSWERS = "question_answers"


PbType = TypeVar("PbType", bound=Message)


class Field(Generic[PbType]):
    pbklass: Type[PbType]
    type: str = "x"
    value: Optional[Any]
    extracted_text: Optional[ExtractedText]
    extracted_vectors: dict[Optional[str], VectorObject]
    computed_metadata: Optional[FieldComputedMetadata]
    large_computed_metadata: Optional[LargeComputedMetadata]
    question_answers: Optional[FieldQuestionAnswers]

    def __init__(
        self,
        id: str,
        resource: Resource,
        pb: Optional[Any] = None,
        value: Optional[Any] = None,
    ):
        if self.pbklass is None:
            raise InvalidFieldClass()

        self.value = None
        self.extracted_text: Optional[ExtractedText] = None
        self.extracted_vectors = {}
        self.computed_metadata = None
        self.large_computed_metadata = None
        self.question_answers = None

        self.id: str = id
        self.resource = resource

        if value is not None:
            newpb = self.pbklass()
            newpb.ParseFromString(value)
            self.value = newpb

        elif pb is not None:
            if not isinstance(pb, self.pbklass):
                raise InvalidPBClass(self.__class__, pb.__class__)
            self.value = pb

        self.locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    @property
    def kbid(self) -> str:
        return self.resource.kb.kbid

    @property
    def uuid(self) -> str:
        return self.resource.uuid

    @property
    def storage(self) -> Storage:
        return self.resource.storage

    @property
    def resource_unique_id(self) -> str:
        return f"{self.uuid}/{self.type}/{self.id}"

    def get_storage_field(self, field_type: FieldTypes) -> StorageField:
        return self.storage.file_extracted(self.kbid, self.uuid, self.type, self.id, field_type.value)

    def _get_extracted_vectors_storage_field(
        self,
        vectorset: str,
        storage_key_kind: VectorSetConfig.StorageKeyKind.ValueType,
    ) -> StorageField:
        if storage_key_kind == VectorSetConfig.StorageKeyKind.LEGACY:
            key = FieldTypes.FIELD_VECTORS.value
        elif storage_key_kind == VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX:
            key = FieldTypes.FIELD_VECTORSET.value.format(vectorset=vectorset)
        else:
            raise ValueError(
                f"Can't do anything with UNSET or unknown vectorset storage key kind: {storage_key_kind}"
            )

        return self.storage.file_extracted(self.kbid, self.uuid, self.type, self.id, key)

    async def db_get_value(self) -> Optional[PbType]:
        if self.value is None:
            payload = await datamanagers.fields.get_raw(
                self.resource.txn,
                kbid=self.kbid,
                rid=self.uuid,
                field_type=self.type,
                field_id=self.id,
            )
            if payload is None:
                return None

            self.value = self.pbklass()
            self.value.ParseFromString(payload)
        return self.value

    async def db_set_value(self, payload: Any):
        await datamanagers.fields.set(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.uuid,
            field_type=self.type,
            field_id=self.id,
            value=payload,
        )
        self.value = payload
        self.resource.modified = True

    async def delete(self):
        await datamanagers.fields.delete(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.uuid,
            field_type=self.type,
            field_id=self.id,
        )
        await self.delete_extracted_text()
        async for vectorset_id, vs in datamanagers.vectorsets.iter(self.resource.txn, kbid=self.kbid):
            await self.delete_vectors(vectorset_id, vs.storage_key_kind)
        await self.delete_metadata()
        await self.delete_question_answers()

    async def delete_question_answers(self) -> None:
        sf = self.get_storage_field(FieldTypes.QUESTION_ANSWERS)
        try:
            await self.storage.delete_upload(sf.key, sf.bucket)
        except KeyError:
            pass

    async def delete_extracted_text(self) -> None:
        sf = self.get_storage_field(FieldTypes.FIELD_TEXT)
        try:
            await self.storage.delete_upload(sf.key, sf.bucket)
        except KeyError:
            pass

    async def delete_vectors(
        self,
        vectorset: str,
        storage_key_kind: VectorSetConfig.StorageKeyKind.ValueType,
    ) -> None:
        # Try delete vectors
        sf = self._get_extracted_vectors_storage_field(vectorset, storage_key_kind)

        if has_feature(const.Features.DEBUG_MISSING_VECTORS):
            # This is a very chatty log. It is just a temporary hint while debugging an issue.
            logger.info(
                "Deleting vectors from storage",
                extra={
                    "kbid": self.kbid,
                    "rid": self.resource.uuid,
                    "field": f"{self.type}/{self.id}",
                    "vectorset": vectorset,
                    "storage_key_kind": storage_key_kind,
                    "key": sf.key,
                    "bucket": sf.bucket,
                },
            )
        try:
            await self.storage.delete_upload(sf.key, sf.bucket)
        except KeyError:
            pass

    async def delete_metadata(self) -> None:
        sf = self.get_storage_field(FieldTypes.FIELD_METADATA)
        try:
            await self.storage.delete_upload(sf.key, sf.bucket)
        except KeyError:
            pass

    async def get_error(self) -> Optional[Error]:
        return await datamanagers.fields.get_error(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.uuid,
            field_type=self.type,
            field_id=self.id,
        )

    async def set_error(self, error: Error) -> None:
        await datamanagers.fields.set_error(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.uuid,
            field_type=self.type,
            field_id=self.id,
            error=error,
        )

    async def get_status(self) -> Optional[FieldStatus]:
        return await datamanagers.fields.get_status(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.uuid,
            field_type=self.type,
            field_id=self.id,
        )

    async def set_status(self, status: FieldStatus) -> None:
        await datamanagers.fields.set_status(
            self.resource.txn,
            kbid=self.kbid,
            rid=self.uuid,
            field_type=self.type,
            field_id=self.id,
            status=status,
        )

    async def get_question_answers(self, force=False) -> Optional[FieldQuestionAnswers]:
        if self.question_answers is None or force:
            sf = self.get_storage_field(FieldTypes.QUESTION_ANSWERS)
            try:
                payload = await self.storage.download_pb(sf, FieldQuestionAnswers)
            except DecodeError:
                deprecated_payload = await self.storage.download_pb(sf, QuestionAnswers)
                if deprecated_payload is not None:
                    payload = FieldQuestionAnswers()
                    payload.question_answers.CopyFrom(deprecated_payload)
            if payload is not None:
                self.question_answers = payload
        return self.question_answers

    async def set_question_answers(self, payload: FieldQuestionAnswerWrapper) -> None:
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[FieldQuestionAnswers] = await self.get_question_answers(
                    force=True
                )
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None
        sf = self.get_storage_field(FieldTypes.QUESTION_ANSWERS)

        if actual_payload is None:
            # Its first question answer
            if payload.HasField("file"):
                await self.storage.normalize_binary(payload.file, sf)
            else:
                await self.storage.upload_pb(sf, payload.question_answers)
                self.question_answers = payload.question_answers
        else:
            if payload.HasField("file"):
                raw_payload = await self.storage.downloadbytescf(payload.file)
                pb = FieldQuestionAnswers()
                pb.ParseFromString(raw_payload.read())
                raw_payload.flush()
                payload.question_answers.CopyFrom(pb)
            # We know its payload.question_answers
            for key, value in payload.question_answers.split_question_answers.items():
                actual_payload.split_question_answers[key] = value
            for key in payload.question_answers.deleted_splits:
                if key in actual_payload.split_question_answers:
                    del actual_payload.split_question_answers[key]
            if payload.question_answers.HasField("question_answers") != "":
                actual_payload.question_answers.CopyFrom(payload.question_answers.question_answers)
            await self.storage.upload_pb(sf, actual_payload)
            self.question_answers = actual_payload

    async def set_extracted_text(self, payload: ExtractedTextWrapper) -> None:
        actual_payload: Optional[ExtractedText] = None
        if self.type in SUBFIELDFIELDS:
            # Try to get the previously extracted text protobuf if it exists so we can merge it with the new splits
            # coming from the processing payload.
            try:
                actual_payload = await self.get_extracted_text(force=True)
            except KeyError:
                # No previous extracted text found
                pass

        sf = self.get_storage_field(FieldTypes.FIELD_TEXT)
        if actual_payload is None:
            # No previous extracted text, this is the first time we set it so we can simply upload it to storage
            if payload.HasField("file"):
                # Normalize the storage key if the payload is a reference to a file in storage.
                # This is typically the case when the text is too large and we store it in a
                # cloud file. Normalization is needed to ensure that the hybrid-onprem deployment stores
                # the file in the correct bucket of its storage.
                await self.storage.normalize_binary(payload.file, sf)
            else:
                # Directly upload the ExtractedText protobuf to storage
                await self.storage.upload_pb(sf, payload.body)
                self.extracted_text = payload.body
        else:
            if payload.HasField("file"):
                # The extracted text coming from processing has a reference to another storage key.
                # Download it and copy it to its ExtractedText.body field. This is typically for cases
                # when the text is too large.
                raw_payload = await self.storage.downloadbytescf(payload.file)
                pb = ExtractedText()
                pb.ParseFromString(raw_payload.read())
                raw_payload.flush()
                payload.body.CopyFrom(pb)

            # Update or set the extracted text text for each split coming from the processing payload
            for key, value in payload.body.split_text.items():
                actual_payload.split_text[key] = value

            # Apply any split deletions that may come in the processing payload
            for key in payload.body.deleted_splits:
                if key in actual_payload.split_text:
                    del actual_payload.split_text[key]

            # Finally, handle the main text body (for the cases where the text is not split)
            if payload.body.text != "":
                actual_payload.text = payload.body.text

            # Upload the updated ExtractedText to storage
            await self.storage.upload_pb(sf, actual_payload)
            self.extracted_text = actual_payload

    async def get_extracted_text(self, force=False) -> Optional[ExtractedText]:
        if self.extracted_text is None or force:
            async with self.locks["extracted_text"]:
                # Value could have been fetched while waiting for the lock
                if self.extracted_text is None or force:
                    sf = self.get_storage_field(FieldTypes.FIELD_TEXT)
                    payload = await self.storage.download_pb(sf, ExtractedText)
                    if payload is not None:
                        self.extracted_text = payload
        return self.extracted_text

    async def set_vectors(
        self,
        payload: ExtractedVectorsWrapper,
        vectorset: str,
        storage_key_kind: VectorSetConfig.StorageKeyKind.ValueType,
    ) -> Optional[VectorObject]:
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[VectorObject] = await self.get_vectors(
                    vectorset=vectorset,
                    storage_key_kind=storage_key_kind,
                    force=True,
                )
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None

        sf = self._get_extracted_vectors_storage_field(vectorset, storage_key_kind)
        vo: Optional[VectorObject] = None
        if actual_payload is None:
            # Its first extracted vectors
            if payload.HasField("file"):
                # When we receive vectors in a cloud file, it points to our
                # storage but paths are different, we may want to move it. This
                # can happen, for example, with LEGACY KBs where processing
                # sends us the extracted vectors prefixed by vectorset but, to
                # maintain bw/c, we move those to the original not prefixed
                # path.
                try:
                    await self.storage.normalize_binary(payload.file, sf)
                except CouldNotCopyNotFound:
                    # A failure here could mean the payload has already been
                    # moved and we're retrying due to a redelivery or another
                    # retry mechanism
                    already_moved = await sf.exists()
                    if already_moved:
                        # We assume is the correct one and do nothing else
                        pass
                    else:
                        raise
                vo = await self.storage.download_pb(sf, VectorObject)
            else:
                await self.storage.upload_pb(sf, payload.vectors)
                vo = payload.vectors
                self.extracted_vectors[vectorset] = payload.vectors
        else:
            if payload.HasField("file"):
                raw_payload = await self.storage.downloadbytescf(payload.file)
                pb = VectorObject()
                pb.ParseFromString(raw_payload.read())
                raw_payload.flush()
                payload.vectors.CopyFrom(pb)
            vo = actual_payload
            # We know its payload.body
            for key, value in payload.vectors.split_vectors.items():
                actual_payload.split_vectors[key].CopyFrom(value)
            for key in payload.vectors.deleted_splits:
                if key in actual_payload.split_vectors:
                    del actual_payload.split_vectors[key]
            if len(payload.vectors.vectors.vectors) > 0:
                actual_payload.vectors.CopyFrom(payload.vectors.vectors)
            await self.storage.upload_pb(sf, actual_payload)
            self.extracted_vectors[vectorset] = actual_payload
        return vo

    async def get_vectors(
        self,
        vectorset: str,
        storage_key_kind: VectorSetConfig.StorageKeyKind.ValueType,
        force: bool = False,
    ) -> Optional[VectorObject]:
        if self.extracted_vectors.get(vectorset, None) is None or force:
            sf = self._get_extracted_vectors_storage_field(vectorset, storage_key_kind)
            payload = await self.storage.download_pb(sf, VectorObject)
            if payload is not None:
                self.extracted_vectors[vectorset] = payload
        return self.extracted_vectors.get(vectorset, None)

    async def set_field_metadata(self, payload: FieldComputedMetadataWrapper) -> FieldComputedMetadata:
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[FieldComputedMetadata] = await self.get_field_metadata(
                    force=True
                )
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None

        sf = self.get_storage_field(FieldTypes.FIELD_METADATA)

        if self.storage.needs_move(payload.metadata.metadata.thumbnail, self.kbid):
            sf_thumbnail = self.get_storage_field(FieldTypes.THUMBNAIL)
            cf: CloudFile = await self.storage.normalize_binary(
                payload.metadata.metadata.thumbnail, sf_thumbnail
            )
            payload.metadata.metadata.thumbnail.CopyFrom(cf)
        payload.metadata.metadata.last_index.FromDatetime(datetime.now())

        for key, metadata in payload.metadata.split_metadata.items():
            if self.storage.needs_move(metadata.thumbnail, self.kbid):
                sf_thumbnail_split = self.get_storage_field(FieldTypes.THUMBNAIL)
                cf_split: CloudFile = await self.storage.normalize_binary(
                    metadata.thumbnail, sf_thumbnail_split
                )
                metadata.thumbnail.CopyFrom(cf_split)
            metadata.last_index.FromDatetime(datetime.now())

        if actual_payload is None:
            # Its first metadata
            await self.storage.upload_pb(sf, payload.metadata)
            self.computed_metadata = payload.metadata
        else:
            # We know its payload.metadata
            for key, value in payload.metadata.split_metadata.items():
                actual_payload.split_metadata[key].CopyFrom(value)
            for key in payload.metadata.deleted_splits:
                if key in actual_payload.split_metadata:
                    del actual_payload.split_metadata[key]
            if payload.metadata.metadata:
                actual_payload.metadata.CopyFrom(payload.metadata.metadata)
            await self.storage.upload_pb(sf, actual_payload)
            self.computed_metadata = actual_payload

        return self.computed_metadata

    async def get_field_metadata(self, force: bool = False) -> Optional[FieldComputedMetadata]:
        if self.computed_metadata is None or force:
            async with self.locks["field_metadata"]:
                # Value could have been fetched while waiting for the lock
                if self.computed_metadata is None or force:
                    sf = self.get_storage_field(FieldTypes.FIELD_METADATA)
                    payload = await self.storage.download_pb(sf, FieldComputedMetadata)
                    if payload is not None:
                        self.computed_metadata = payload
        return self.computed_metadata

    async def set_large_field_metadata(self, payload: LargeComputedMetadataWrapper):
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[LargeComputedMetadata] = await self.get_large_field_metadata(
                    force=True
                )
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None

        sf = self.get_storage_field(FieldTypes.FIELD_LARGE_METADATA)

        new_payload: Optional[LargeComputedMetadata] = None
        if payload.HasField("file"):
            new_payload = LargeComputedMetadata()
            data = await self.storage.downloadbytescf(payload.file)
            new_payload.ParseFromString(data.read())
            data.flush()
        else:
            new_payload = payload.real

        if actual_payload is None:
            # Its first metadata
            await self.storage.upload_pb(sf, new_payload)
            self.large_computed_metadata = new_payload
        else:
            for key, value in new_payload.split_metadata.items():
                actual_payload.split_metadata[key].CopyFrom(value)

            for key in new_payload.deleted_splits:
                if key in actual_payload.split_metadata:
                    del actual_payload.split_metadata[key]
            if new_payload.metadata:
                actual_payload.metadata.CopyFrom(new_payload.metadata)
            await self.storage.upload_pb(sf, actual_payload)
            self.large_computed_metadata = actual_payload

        return self.large_computed_metadata

    async def get_large_field_metadata(self, force: bool = False) -> Optional[LargeComputedMetadata]:
        if self.large_computed_metadata is None or force:
            sf = self.get_storage_field(FieldTypes.FIELD_LARGE_METADATA)
            payload = await self.storage.download_pb(
                sf,
                LargeComputedMetadata,
            )
            if payload is not None:
                self.large_computed_metadata = payload
        return self.large_computed_metadata

    async def generated_by(self) -> FieldAuthor:
        author = FieldAuthor()
        author.user.SetInParent()
        return author

    def serialize(self):
        return self.value.SerializeToString()

    async def set_value(self, value: Any):
        raise NotImplementedError()

    async def get_value(self) -> Any:
        raise NotImplementedError()
