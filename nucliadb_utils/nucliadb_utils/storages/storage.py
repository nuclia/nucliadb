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

import abc
import hashlib
from io import BytesIO
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List, Optional, Tuple, Type

from nucliadb_protos.noderesources_pb2 import Resource as BrainResource
from nucliadb_protos.nodewriter_pb2 import IndexMessage
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_utils import logger
from nucliadb_utils.storages import CHUNK_SIZE
from nucliadb_utils.storages.exceptions import InvalidCloudFile
from nucliadb_utils.utilities import get_local_storage, get_nuclia_storage

STORAGE_RESOURCE = "kbs/{kbid}/r/{uuid}"
KB_RESOURCE_FIELD = "kbs/{kbid}/r/{uuid}/f/f/{field}"
KB_LAYOUT_FIELD = "kbs/{kbid}/r/{uuid}/f/l/{field}/{ident}"
KB_CONVERSATION_FIELD = "kbs/{kbid}/r/{uuid}/f/c/{field}/{ident}/{count}"
STORAGE_FILE_EXTRACTED = "kbs/{kbid}/r/{uuid}/e/{field_type}/{field}/{key}"

DEADLETTER = "deadletter/{partition}/{seqid}/{seq}"
INDEXING = "index/{node}/{shard}/{txid}"


class StorageField:
    storage: Storage
    bucket: str
    key: str
    field: Optional[CloudFile] = None

    def __init__(
        self,
        storage: Storage,
        bucket: str,
        fullkey: str,
        field: Optional[CloudFile] = None,
    ):
        self.storage = storage
        self.bucket = bucket
        self.key = fullkey
        self.field = field

    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile:
        raise NotImplementedError()

    async def iter_data(self, headers=None):
        raise NotImplementedError()

    async def delete(self) -> bool:
        deleted = False
        if self.field is not None:
            await self.storage.delete_upload(self.bucket, self.field.uri)
            deleted = True
        return deleted

    async def exists(self) -> Optional[Dict[str, str]]:
        raise NotImplementedError

    def build_cf(self) -> CloudFile:
        cf = CloudFile()
        cf.bucket_name = self.bucket
        cf.uri = self.key
        cf.filename = "payload.pb"
        cf.source = self.storage.source  # type: ignore
        return cf


class Storage:
    source: int
    field_klass: Type
    deadletter_bucket: Optional[str] = None
    indexing_bucket: Optional[str] = None
    cached_buckets: List[str] = []

    async def delete_resource(self, kbid: str, uuid: str):
        # Delete all keys inside a resource
        bucket = self.get_bucket_name(kbid)
        resource_storage_base_path = STORAGE_RESOURCE.format(kbid=kbid, uuid=uuid)
        async for bucket_info in self.iterate_bucket(
            bucket, resource_storage_base_path
        ):
            await self.delete_upload(bucket_info["name"], bucket)

    async def deadletter(
        self, message: BrokerMessage, seq: int, seqid: int, partition: str
    ):
        if self.deadletter_bucket is None:
            logger.error("No Deadletter Bucket defined will not store the error")
            return
        key = DEADLETTER.format(seqid=seqid, seq=seq, partition=partition)
        await self.uploadbytes(self.deadletter_bucket, key, message.SerializeToString())

    async def indexing(
        self, message: BrainResource, node: str, shard: str, txid: int
    ) -> IndexMessage:
        if self.indexing_bucket is None:
            raise AttributeError()
        if txid < 0:
            txid = 0
        key = INDEXING.format(node=node, shard=shard, txid=txid)
        await self.uploadbytes(self.indexing_bucket, key, message.SerializeToString())
        response = IndexMessage()
        response.node = node
        response.shard = shard
        response.txid = txid
        response.typemessage = IndexMessage.TypeMessage.CREATION
        return response

    async def reindexing(
        self, message: BrainResource, node: str, shard: str, reindex_id: str
    ) -> IndexMessage:
        if self.indexing_bucket is None:
            raise AttributeError()
        key = INDEXING.format(node=node, shard=shard, txid=reindex_id)
        await self.uploadbytes(self.indexing_bucket, key, message.SerializeToString())
        response = IndexMessage()
        response.node = node
        response.shard = shard
        response.reindex_id = reindex_id
        response.typemessage = IndexMessage.TypeMessage.CREATION
        return response

    async def get_indexing(self, payload: IndexMessage) -> BrainResource:
        if self.indexing_bucket is None:
            raise AttributeError()
        if payload.txid == 0:
            key = INDEXING.format(
                node=payload.node, shard=payload.shard, txid=payload.reindex_id
            )
        else:
            key = INDEXING.format(
                node=payload.node, shard=payload.shard, txid=payload.txid
            )
        bytes_buffer = await self.downloadbytes(self.indexing_bucket, key)
        if bytes_buffer.getbuffer().nbytes == 0:
            raise KeyError()
        pb = BrainResource()
        pb.ParseFromString(bytes_buffer.read())
        bytes_buffer.flush()
        return pb

    async def delete_indexing(self, payload: IndexMessage):
        if self.indexing_bucket is None:
            raise AttributeError()
        key = INDEXING.format(node=payload.node, shard=payload.shard, txid=payload.txid)
        await self.delete_upload(key, self.indexing_bucket)

    def needs_move(self, file: CloudFile, kbid: str) -> bool:
        # The cloudfile is valid for our environment
        if file.uri == "":
            return False
        elif (
            file.source == self.source
            and self.get_bucket_name(kbid) == file.bucket_name
        ):
            return False
        else:
            return True

    async def normalize_binary(self, file: CloudFile, destination: StorageField):
        # see if file is in the same Cloud in the same bucket
        if file.source == self.source and file.uri != destination.key:
            new_cf = await self.move(file, destination)
        elif file.source == self.source:
            return file

        elif file.source == CloudFile.FLAPS:
            flaps_storage = await get_nuclia_storage()
            iterator = flaps_storage.download(file)
            new_cf = await self.uploaditerator(iterator, destination, file)
        elif file.source == CloudFile.LOCAL:
            local_storage = get_local_storage()
            iterator = local_storage.download(file.bucket_name, file.uri)
            new_cf = await self.uploaditerator(iterator, destination, file)
        elif file.source == CloudFile.EMPTY:
            new_cf = CloudFile()
            new_cf.CopyFrom(file)
        else:
            raise InvalidCloudFile()
        return new_cf

    def conversation_field(
        self, kbid: str, uuid: str, field: str, ident: str, count: int
    ) -> StorageField:
        bucket = self.get_bucket_name(kbid)
        key = KB_CONVERSATION_FIELD.format(
            kbid=kbid, uuid=uuid, field=field, ident=ident, count=count
        )
        return self.field_klass(storage=self, bucket=bucket, fullkey=key)

    def layout_field(
        self, kbid: str, uuid: str, field: str, ident: str
    ) -> StorageField:
        bucket = self.get_bucket_name(kbid)
        key = KB_LAYOUT_FIELD.format(kbid=kbid, uuid=uuid, field=field, ident=ident)
        return self.field_klass(storage=self, bucket=bucket, fullkey=key)

    def file_field(
        self,
        kbid: str,
        uuid: str,
        field: str,
        old_field: Optional[CloudFile] = None,
    ) -> StorageField:
        # Its a file field value
        bucket = self.get_bucket_name(kbid)
        key = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=uuid, field=field)
        return self.field_klass(
            storage=self, bucket=bucket, fullkey=key, field=old_field
        )

    def file_extracted(
        self, kbid: str, uuid: str, field_type: str, field: str, key: str
    ) -> StorageField:
        # Its a file field value
        bucket = self.get_bucket_name(kbid)
        key = STORAGE_FILE_EXTRACTED.format(
            kbid=kbid, uuid=uuid, field_type=field_type, field=field, key=key
        )
        return self.field_klass(storage=self, bucket=bucket, fullkey=key)

    async def upload_b64file_to_cloudfile(
        self,
        sf: StorageField,
        payload: bytes,
        filename: str,
        content_type: str,
        md5: Optional[str] = None,
    ):
        cf = CloudFile()
        cf.filename = filename
        cf.content_type = content_type
        cf.size = len(payload)
        cf.source = self.source  # type: ignore

        if md5 is None:
            md5hash = hashlib.md5(payload).digest()
            cf.md5 = md5hash.decode()
        else:
            cf.md5 = md5
        buffer = BytesIO(payload)

        async def splitter(alldata: BytesIO):
            while True:
                data = alldata.read(CHUNK_SIZE)
                if data == b"":
                    break
                yield data

        generator = splitter(buffer)
        cf = await self.uploaditerator(generator, sf, cf)
        return cf

    async def uploadbytes(self, bucket: str, key: str, payload: bytes):
        destination = self.field_klass(storage=self, bucket=bucket, fullkey=key)

        cf = CloudFile()
        cf.filename = "payload"
        cf.size = len(payload)
        buffer = BytesIO(payload)

        async def splitter(alldata: BytesIO):
            while True:
                data = alldata.read(CHUNK_SIZE)
                if data == b"":
                    break
                yield data

        generator = splitter(buffer)
        await self.uploaditerator(generator, destination, cf)

    async def uploaditerator(
        self, iterator: AsyncIterator, destination: StorageField, origin: CloudFile
    ) -> CloudFile:
        return await destination.upload(iterator, origin)

    async def download(
        self, bucket: str, key: str, headers: Optional[Dict[str, str]] = None
    ):
        destination: StorageField = self.field_klass(
            storage=self, bucket=bucket, fullkey=key
        )
        if headers is None:
            headers = {}

        try:
            async for data in destination.iter_data(headers=headers):
                yield data
        except KeyError:
            yield None

    async def downloadbytes(self, bucket: str, key: str) -> BytesIO:
        result = BytesIO()
        async for data in self.download(bucket, key):
            if data is not None:
                result.write(data)

        result.seek(0)
        return result

    async def downloadbytescf(self, cf: CloudFile) -> BytesIO:
        result = BytesIO()
        if cf.source == self.source:
            async for data in self.download(cf.bucket_name, cf.uri):
                if data is not None:
                    result.write(data)
        elif cf.source == CloudFile.FLAPS:
            flaps_storage = await get_nuclia_storage()
            async for data in flaps_storage.download(cf):
                if data is not None:
                    result.write(data)
        elif cf.source == CloudFile.LOCAL:
            local_storage = get_local_storage()
            async for data in local_storage.download(cf.bucket_name, cf.uri):
                if data is not None:
                    result.write(data)

        result.seek(0)
        return result

    async def downloadbytescf_iterator(
        self, cf: CloudFile
    ) -> AsyncGenerator[bytes, None]:
        if cf.source == self.source:
            async for data in self.download(cf.bucket_name, cf.uri):
                if data is not None:
                    yield data
        elif cf.source == CloudFile.FLAPS:
            flaps_storage = await get_nuclia_storage()
            async for data in flaps_storage.download(cf):
                if data is not None:
                    yield data
        elif cf.source == CloudFile.LOCAL:
            local_storage = get_local_storage()
            async for data in local_storage.download(cf.bucket_name, cf.uri):
                if data is not None:
                    yield data

    async def upload_pb(self, sf: StorageField, payload: Any):
        await self.uploadbytes(sf.bucket, sf.key, payload.SerializeToString())

    async def download_pb(self, sf: StorageField, PBKlass: Type):
        payload = await self.downloadbytes(sf.bucket, sf.key)

        if payload.getbuffer().nbytes == 0:
            return None

        pb = PBKlass()
        pb.ParseFromString(payload.read())
        return pb

    async def delete_upload(self, uri: str, bucket_name: str):
        raise NotImplementedError()

    def get_bucket_name(self, kbid: str):
        raise NotImplementedError()

    async def initialize(self):
        raise NotImplementedError()

    async def finalize(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def iterate_bucket(self, bucket: str, prefix: str) -> AsyncIterator[Any]:
        raise NotImplementedError()

    async def move(self, file: CloudFile, destination: StorageField):
        raise NotImplementedError()

    async def create_kb(self, kbid: str) -> bool:
        raise NotImplementedError()

    async def delete_kb(self, kbid: str) -> Tuple[bool, bool]:
        raise NotImplementedError()

    async def schedule_delete_kb(self, kbid: str) -> bool:
        raise NotImplementedError()
