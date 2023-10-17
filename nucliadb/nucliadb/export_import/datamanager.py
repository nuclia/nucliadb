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
from datetime import datetime
from typing import AsyncGenerator, Union

from nucliadb.common.maindb.driver import Driver
from nucliadb.export_import import logger
from nucliadb.export_import.exceptions import MetadataNotFound
from nucliadb.export_import.models import ExportMetadata, ImportMetadata
from nucliadb_protos import resources_pb2
from nucliadb_telemetry import errors
from nucliadb_utils.helpers import async_gen_lookahead
from nucliadb_utils.storages.storage import Storage, StorageField

MAINDB_EXPORT_KEY = "/kbs/{kbid}/exports/{id}"
MAINDB_IMPORT_KY = "/kbs/{kbid}/imports/{id}"
STORAGE_EXPORT_KEY = "exports/{export_id}"
STORAGE_IMPORT_KEY = "imports/{import_id}"

Metadata = Union[ExportMetadata, ImportMetadata]


class ExportImportDataManager:
    """
    Manages data layer to store/retrieve the metadata of exports and imports.
    """

    def __init__(self, driver: Driver, storage: Storage):
        self.driver = driver
        self.storage = storage

    def _get_maindb_metadata_key(self, type: str, kbid: str, id: str) -> str:
        if type not in ("export", "import"):
            raise ValueError(f"Invalid type: {type}")
        key = MAINDB_EXPORT_KEY if type == "export" else MAINDB_IMPORT_KY
        return key.format(kbid=kbid, id=id)

    async def get_metadata(self, type: str, kbid: str, id: str) -> Metadata:
        key = self._get_maindb_metadata_key(type, kbid, id)
        async with self.driver.transaction() as txn:
            data = await txn.get(key)
        if data is None or data == b"":
            raise MetadataNotFound()
        decoded = data.decode("utf-8")
        model_type = {
            "export": ExportMetadata,
            "import": ImportMetadata,
        }[type]
        return model_type.parse_raw(decoded)  # type: ignore

    async def set_metadata(
        self,
        type: str,
        metadata: Metadata,
    ):
        metadata.modified = datetime.utcnow()
        key = self._get_maindb_metadata_key(type, metadata.kbid, metadata.id)
        data = metadata.json().encode("utf-8")
        async with self.driver.transaction() as txn:
            await txn.set(key, data)
            await txn.commit()

    async def delete_metadata(self, type: str, metadata: Metadata):
        key = self._get_maindb_metadata_key(type, metadata.kbid, metadata.id)
        async with self.driver.transaction() as txn:
            await txn.delete(key)
            await txn.commit()

    async def upload_export(
        self,
        export_bytes: AsyncGenerator[bytes, None],
        kbid: str,
        export_id: str,
    ):
        key = STORAGE_EXPORT_KEY.format(export_id=export_id)
        cf = resources_pb2.CloudFile()
        cf.bucket_name = self.storage.get_bucket_name(kbid)
        cf.content_type = "binary/octet-stream"
        cf.source = resources_pb2.CloudFile.Source.EXPORT
        field: StorageField = self._get_storage_field(kbid, key, cf)
        iterator = iterate_storage_compatible(export_bytes, self.storage, cf)
        await self.storage.uploaditerator(iterator, field, cf)

    async def download_export(
        self, kbid: str, export_id: str
    ) -> AsyncGenerator[bytes, None]:
        key = STORAGE_EXPORT_KEY.format(export_id=export_id)
        bucket = self.storage.get_bucket_name(kbid)
        async for chunk in self.storage.download(bucket, key):
            yield chunk

    async def upload_import(
        self,
        import_bytes: AsyncGenerator[bytes, None],
        kbid: str,
        import_id: str,
    ):
        key = STORAGE_IMPORT_KEY.format(import_id=import_id)
        cf = resources_pb2.CloudFile()
        cf.bucket_name = self.storage.get_bucket_name(kbid)
        cf.content_type = "binary/octet-stream"
        field: StorageField = self._get_storage_field(kbid, key, cf)
        iterator = iterate_storage_compatible(import_bytes, self.storage, cf)
        await self.storage.uploaditerator(iterator, field, cf)

    async def download_import(self, kbid: str, import_id: str):
        key = STORAGE_IMPORT_KEY.format(import_id=import_id)
        bucket = self.storage.get_bucket_name(kbid)
        async for chunk in self.storage.download(bucket, key):
            yield chunk

    def _get_storage_field(
        self, kbid: str, key: str, cf: resources_pb2.CloudFile
    ) -> StorageField:
        bucket = self.storage.get_bucket_name(kbid)
        return self.storage.field_klass(
            storage=self.storage, bucket=bucket, fullkey=key, field=cf
        )

    async def delete_import(self, kbid: str, import_id: str):
        key = STORAGE_IMPORT_KEY.format(import_id=import_id)
        bucket = self.storage.get_bucket_name(kbid)
        await self.storage.delete_upload(key, bucket_name=bucket)

    async def delete_export(self, kbid: str, export_id: str):
        key = STORAGE_EXPORT_KEY.format(export_id=export_id)
        bucket = self.storage.get_bucket_name(kbid)
        await self.storage.delete_upload(key, bucket_name=bucket)

    async def try_delete_from_storage(self, type: str, kbid: str, id: str):
        if type not in ("export", "import"):
            raise ValueError(f"Invalid type: {type}")
        func = self.delete_export if type == "export" else self.delete_import
        try:
            await func(kbid, id)
        except Exception as ex:
            errors.capture_exception(ex)
            logger.exception(
                f"Could not delete {type} {id} from storage", extra={"kbid": kbid}
            )


async def iter_and_add_size(
    stream: AsyncGenerator[bytes, None], cf: resources_pb2.CloudFile
) -> AsyncGenerator[bytes, None]:
    # This is needed to upload exports to GCS because it requires the size of
    # the file at least at the request for the last chunk.
    total_size = 0
    async for chunk, is_last in async_gen_lookahead(stream):
        total_size += len(chunk)
        if is_last:
            cf.size = total_size
        yield chunk


async def iter_in_chunk_size(
    stream: AsyncGenerator[bytes, None], chunk_size: int
) -> AsyncGenerator[bytes, None]:
    # This is needed to make sure bytes uploaded to the blob storage complies with a particular chunk size.
    buffer = b""
    async for chunk in stream:
        buffer += chunk
        if len(buffer) >= chunk_size:
            yield buffer[:chunk_size]
            buffer = buffer[chunk_size:]
    # The last chunk can be smaller than chunk size
    if len(buffer) > 0:
        yield buffer


async def iterate_storage_compatible(
    stream: AsyncGenerator[bytes, None], storage: Storage, cf: resources_pb2.CloudFile
) -> AsyncGenerator[bytes, None]:
    async for chunk in iter_in_chunk_size(
        iter_and_add_size(stream, cf), chunk_size=storage.chunk_size
    ):
        yield chunk
