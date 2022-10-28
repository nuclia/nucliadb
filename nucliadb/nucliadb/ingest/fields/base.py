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

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Type

from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadata,
    FieldComputedMetadataWrapper,
    LargeComputedMetadata,
    LargeComputedMetadataWrapper,
)
from nucliadb_protos.utils_pb2 import ExtractedText, VectorObject
from nucliadb_protos.writer_pb2 import Error

from nucliadb.ingest.fields.exceptions import InvalidFieldClass, InvalidPBClass
from nucliadb_utils.storages.storage import Storage, StorageField

KB_RESOURCE_FIELD = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"
KB_RESOURCE_ERROR = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/error"
FIELD_TEXT = "extracted_text"
FIELD_VECTORS = "extracted_vectors"
FIELD_METADATA = "metadata"
FIELD_LARGE_METADATA = "large_metadata"
SUBFIELDFIELDS = ["l", "c"]


class Field:
    pbklass: Optional[Type] = None
    type: str = "x"
    value: Optional[Any]
    extracted_text: Optional[ExtractedText]
    extracted_vectors: Optional[VectorObject]
    computed_metadata: Optional[FieldComputedMetadata]
    large_computed_metadata: Optional[LargeComputedMetadata]

    def __init__(
        self,
        id: str,
        resource: Any,
        pb: Optional[Any] = None,
        value: Optional[Any] = None,
    ):
        if self.pbklass is None:
            raise InvalidFieldClass()

        self.value = None
        self.extracted_text = None
        self.extracted_vectors = None
        self.computed_metadata = None
        self.large_computed_metadata = None

        self.id: str = id
        self.resource: Any = resource

        if value is not None:
            newpb = self.pbklass()
            newpb.ParseFromString(value)
            self.value = newpb

        elif pb is not None:
            if not isinstance(pb, self.pbklass):
                raise InvalidPBClass(self.__class__, pb.__class__)
            self.value = pb

    @property
    def kbid(self) -> str:
        return self.resource.kb.kbid

    @property
    def uuid(self) -> str:
        return self.resource.uuid

    @property
    def storage(self) -> Storage:
        return self.resource.storage

    async def db_get_value(self):
        if self.value is None:

            payload = await self.resource.txn.get(
                KB_RESOURCE_FIELD.format(
                    kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id
                )
            )
            if payload is None:
                return

            self.value = self.pbklass()
            self.value.ParseFromString(payload)
        return self.value

    async def db_set_value(self, payload: Any):
        await self.resource.txn.set(
            KB_RESOURCE_FIELD.format(
                kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id
            ),
            payload.SerializeToString(),
        )
        self.value = payload
        self.resource.modified = True

    async def delete(self):
        field_base_key = KB_RESOURCE_FIELD.format(
            kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id
        )
        # Make sure we explicitly delete the field and any nested key
        keys_to_delete = []
        async for key in self.resource.txn.keys(field_base_key):
            keys_to_delete.append(key)
        for key in keys_to_delete:
            await self.resource.txn.delete(key)
        await self.delete_extracted_text()
        await self.delete_vectors()
        await self.delete_metadata()

    async def delete_extracted_text(self):
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_TEXT
        )
        try:
            await self.storage.delete_upload(sf.key, sf.bucket)
        except KeyError:
            pass

    async def delete_vectors(self):
        # Try delete vectors
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_VECTORS
        )
        try:
            await self.storage.delete_upload(sf.key, sf.bucket)
        except KeyError:
            pass

    async def delete_metadata(self):
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_METADATA
        )
        try:
            await self.storage.delete_upload(sf.key, sf.bucket)
        except KeyError:
            pass

    async def get_error(self) -> Optional[Error]:
        payload = await self.resource.txn.get(
            KB_RESOURCE_ERROR.format(
                kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id
            )
        )
        if payload is None:
            return None
        pberror = Error()
        pberror.ParseFromString(payload)
        return pberror

    async def set_error(self, error: Error):
        await self.resource.txn.set(
            KB_RESOURCE_ERROR.format(
                kbid=self.kbid, uuid=self.uuid, type=self.type, field=self.id
            ),
            error.SerializeToString(),
        )

    async def set_extracted_text(self, payload: ExtractedTextWrapper):
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[ExtractedText] = await self.get_extracted_text(
                    force=True
                )
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_TEXT
        )

        if actual_payload is None:
            # Its first extracted text
            if payload.HasField("file"):
                await self.storage.normalize_binary(payload.file, sf)
            else:
                await self.storage.upload_pb(sf, payload.body)
                self.extracted_text = payload.body
        else:
            if payload.HasField("file"):
                raw_payload = await self.storage.downloadbytescf(payload.file)
                pb = ExtractedText()
                pb.ParseFromString(raw_payload.read())
                raw_payload.flush()
                payload.body.CopyFrom(pb)
            # We know its payload.body
            for key, value in payload.body.split_text.items():
                actual_payload.split_text[key] = value
            for key in payload.body.deleted_splits:
                if key in actual_payload.split_text:
                    del actual_payload.split_text[key]
            if payload.body.text != "":
                actual_payload.text = payload.body.text
            await self.storage.upload_pb(sf, actual_payload)
            self.extracted_text = actual_payload

    async def get_extracted_text(self, force=False) -> Optional[ExtractedText]:
        if self.extracted_text is None or force:
            sf: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, FIELD_TEXT
            )
            payload = await self.storage.download_pb(sf, ExtractedText)
            if payload is not None:
                self.extracted_text = payload
        return self.extracted_text

    async def get_extracted_text_cf(self) -> Optional[CloudFile]:
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_TEXT
        )
        if await sf.exists() is not None:
            return sf.build_cf()
        else:
            return None

    async def set_vectors(
        self, payload: ExtractedVectorsWrapper
    ) -> Tuple[Optional[VectorObject], bool, List[str]]:
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[VectorObject] = await self.get_vectors(
                    force=True
                )
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None

        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_VECTORS
        )

        vo: Optional[VectorObject] = None
        replace_field: bool = True
        replace_splits = []
        if actual_payload is None:
            # Its first extracted text
            if payload.HasField("file"):
                await self.storage.normalize_binary(payload.file, sf)
                vo = await self.storage.download_pb(sf, VectorObject)
            else:
                await self.storage.upload_pb(sf, payload.vectors)
                vo = payload.vectors
                self.extracted_vectors = payload.vectors
        else:
            if payload.HasField("file"):
                raw_payload = await self.storage.downloadbytescf(payload.file)
                pb = VectorObject()
                pb.ParseFromString(raw_payload.read())
                raw_payload.flush()
                payload.vectors.CopyFrom(pb)
            vo = payload.vectors
            # We know its payload.body
            for key, value in payload.vectors.split_vectors.items():
                actual_payload.split_vectors[key].CopyFrom(value)
            for key in payload.vectors.deleted_splits:
                if key in actual_payload.split_vectors:
                    replace_splits.append(key)
                    del actual_payload.split_vectors[key]
            if len(payload.vectors.vectors.vectors) > 0:
                replace_field = True
                actual_payload.vectors.CopyFrom(payload.vectors.vectors)
            await self.storage.upload_pb(sf, actual_payload)
            self.extracted_vectors = actual_payload
        return vo, replace_field, replace_splits

    async def get_vectors(self, force=False) -> Optional[VectorObject]:
        if self.extracted_vectors is None or force:
            sf: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, FIELD_VECTORS
            )
            payload = await self.storage.download_pb(sf, VectorObject)
            if payload is not None:
                self.extracted_vectors = payload
        return self.extracted_vectors

    async def get_vectors_cf(self) -> Optional[CloudFile]:
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_VECTORS
        )
        if await sf.exists() is not None:
            return sf.build_cf()
        else:
            return None

    async def set_field_metadata(
        self, payload: FieldComputedMetadataWrapper
    ) -> Tuple[FieldComputedMetadata, List[str], Dict[str, List[str]]]:
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[
                    FieldComputedMetadata
                ] = await self.get_field_metadata(force=True)
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None

        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_METADATA
        )

        if self.storage.needs_move(payload.metadata.metadata.thumbnail, self.kbid):
            sf_thumbnail: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, f"thumbnail"
            )
            cf: CloudFile = await self.storage.normalize_binary(
                payload.metadata.metadata.thumbnail, sf_thumbnail
            )
            payload.metadata.metadata.thumbnail.CopyFrom(cf)
        payload.metadata.metadata.last_index.FromDatetime(datetime.now())

        for key, metadata in payload.metadata.split_metadata.items():
            if self.storage.needs_move(metadata.thumbnail, self.kbid):
                sf_thumbnail_split: StorageField = self.storage.file_extracted(
                    self.kbid, self.uuid, self.type, self.id, f"thumbnail"
                )
                cf_split: CloudFile = await self.storage.normalize_binary(
                    metadata.thumbnail, sf_thumbnail_split
                )
                metadata.thumbnail.CopyFrom(cf_split)
            metadata.last_index.FromDatetime(datetime.now())

        replace_field = []
        replace_splits = {}
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
                    replace_splits[key] = [
                        f"{x.start}-{x.end}"
                        for x in actual_payload.split_metadata[key].paragraphs
                    ]
                    del actual_payload.split_metadata[key]
            if payload.metadata.metadata:
                actual_payload.metadata.CopyFrom(payload.metadata.metadata)
                replace_field = [f"{x.start}-{x.end}" for x in metadata.paragraphs]
            await self.storage.upload_pb(sf, actual_payload)
            self.computed_metadata = actual_payload

        return self.computed_metadata, replace_field, replace_splits

    async def get_field_metadata(
        self, force: bool = False
    ) -> Optional[FieldComputedMetadata]:
        if self.computed_metadata is None or force:
            sf: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, FIELD_METADATA
            )
            payload = await self.storage.download_pb(sf, FieldComputedMetadata)
            if payload is not None:
                self.computed_metadata = payload
        return self.computed_metadata

    async def get_field_metadata_cf(self) -> Optional[CloudFile]:
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_METADATA
        )
        if await sf.exists() is not None:
            return sf.build_cf()
        else:
            return None

    async def set_large_field_metadata(self, payload: LargeComputedMetadataWrapper):
        if self.type in SUBFIELDFIELDS:
            try:
                actual_payload: Optional[
                    LargeComputedMetadata
                ] = await self.get_large_field_metadata(force=True)
            except KeyError:
                actual_payload = None
        else:
            actual_payload = None

        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_LARGE_METADATA
        )

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

    async def get_large_field_metadata(
        self, force: bool = False
    ) -> Optional[LargeComputedMetadata]:
        if self.large_computed_metadata is None or force:
            sf: StorageField = self.storage.file_extracted(
                self.kbid, self.uuid, self.type, self.id, FIELD_LARGE_METADATA
            )
            payload = await self.storage.download_pb(
                sf,
                LargeComputedMetadata,
            )
            if payload is not None:
                self.large_computed_metadata = payload
        return self.large_computed_metadata

    async def get_large_field_metadata_cf(self) -> Optional[CloudFile]:
        sf: StorageField = self.storage.file_extracted(
            self.kbid, self.uuid, self.type, self.id, FIELD_LARGE_METADATA
        )
        if await sf.exists() is not None:
            return sf.build_cf()
        else:
            return None

    def serialize(self):
        return self.value.SerializeToString()

    async def set_value(self, value: Any):
        raise NotImplementedError()

    async def get_value(self) -> Any:
        raise NotImplementedError()
