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
import dataclasses
import hashlib
import json
import uuid
from io import BytesIO
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from nucliadb_protos.noderesources_pb2 import Resource as BrainResource
from nucliadb_protos.nodewriter_pb2 import IndexMessage
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils import logger
from nucliadb_utils.helpers import async_gen_lookahead
from nucliadb_utils.storages import CHUNK_SIZE, DEFAULT_CHUNK_SIZE
from nucliadb_utils.storages.exceptions import IndexDataNotFound, InvalidCloudFile
from nucliadb_utils.utilities import get_local_storage, get_nuclia_storage

STORAGE_RESOURCE = "kbs/{kbid}/r/{uuid}"
KB_RESOURCE_FIELD = "kbs/{kbid}/r/{uuid}/f/f/{field}"
KB_LAYOUT_FIELD = "kbs/{kbid}/r/{uuid}/f/l/{field}/{ident}"
KB_CONVERSATION_FIELD = "kbs/{kbid}/r/{uuid}/f/c/{field}/{ident}/{count}"
STORAGE_FILE_EXTRACTED = "kbs/{kbid}/r/{uuid}/e/{field_type}/{field}/{key}"

DEADLETTER = "deadletter/{partition}/{seqid}/{seq}"
OLD_INDEXING_KEY = "index/{node}/{shard}/{txid}"
INDEXING_KEY = "index/{kb}/{shard}/{resource}/{txid}"
# temporary storage for large stream data
MESSAGE_KEY = "message/{kbid}/{rid}/{mid}"


@dataclasses.dataclass
class ObjectInfo:
    name: str


@dataclasses.dataclass
class ObjectMetadata:
    filename: str
    content_type: str
    size: int

    @classmethod
    def parse_raw(cls, raw: bytes) -> ObjectMetadata:
        decoded = cast(dict, json.loads(raw))
        lowercase = {k.lower(): v for k, v in decoded.items()}
        return cls(**lowercase)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


class StorageField(abc.ABC, metaclass=abc.ABCMeta):
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

    @abc.abstractmethod
    async def upload(self, iterator: AsyncIterator, origin: CloudFile) -> CloudFile: ...

    @abc.abstractmethod
    async def iter_data(self, headers=None) -> AsyncGenerator[bytes, None]:  # type: ignore
        raise NotImplementedError()
        yield b""

    @abc.abstractmethod
    async def read_range(self, start: int, end: int) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError()
        yield b""

    async def delete(self) -> bool:
        deleted = False
        if self.field is not None:
            await self.storage.delete_upload(self.field.uri, self.bucket)
            deleted = True
        return deleted

    @abc.abstractmethod
    async def exists(self) -> Optional[ObjectMetadata]: ...

    @abc.abstractmethod
    async def copy(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ): ...

    @abc.abstractmethod
    async def move(
        self,
        origin_uri: str,
        destination_uri: str,
        origin_bucket_name: str,
        destination_bucket_name: str,
    ): ...

    @abc.abstractmethod
    async def start(self, cf: CloudFile) -> CloudFile: ...

    @abc.abstractmethod
    async def append(self, cf: CloudFile, iterable: AsyncIterator) -> int: ...

    @abc.abstractmethod
    async def finish(self): ...


class Storage:
    field_klass: Type

    def __init__(
        self,
        raw_driver: RawStorage,
        deadletter_bucket: str,
        indexing_bucket: str,
    ):
        self.raw_driver = raw_driver
        self.deadletter_bucket = deadletter_bucket
        self.indexing_bucket = indexing_bucket

    @property
    def source(self) -> int:
        return self.raw_driver.source

    @property
    def chunk_size(self) -> int:
        return self.raw_driver.chunk_size

    async def delete_resource(self, kbid: str, uuid: str):
        # Delete all keys inside a resource
        bucket = self.get_bucket_name(kbid)
        resource_storage_base_path = STORAGE_RESOURCE.format(kbid=kbid, uuid=uuid)
        async for object_info in self.iterate_objects(
            bucket, resource_storage_base_path
        ):
            await self.delete_upload(object_info.name, bucket)

    async def deadletter(
        self, message: BrokerMessage, seq: int, seqid: int, partition: str
    ):
        if self.deadletter_bucket is None:
            logger.error("No Deadletter Bucket defined will not store the error")
            return
        key = DEADLETTER.format(seqid=seqid, seq=seq, partition=partition)
        await self.uploadbytes(self.deadletter_bucket, key, message.SerializeToString())

    def get_indexing_storage_key(
        self, *, kb: str, logical_shard: str, resource_uid: str, txid: Union[int, str]
    ):
        return INDEXING_KEY.format(
            kb=kb, shard=logical_shard, resource=resource_uid, txid=txid
        )

    async def indexing(
        self,
        message: BrainResource,
        txid: int,
        partition: Optional[str],
        kb: str,
        logical_shard: str,
    ) -> str:
        if self.indexing_bucket is None:
            raise AttributeError()
        if txid < 0:
            txid = 0

        key = self.get_indexing_storage_key(
            kb=kb,
            logical_shard=logical_shard,
            resource_uid=message.resource.uuid,
            txid=txid,
        )
        await self.uploadbytes(self.indexing_bucket, key, message.SerializeToString())

        return key

    async def reindexing(
        self,
        message: BrainResource,
        reindex_id: str,
        partition: Optional[str],
        kb: str,
        logical_shard: str,
    ) -> str:
        if self.indexing_bucket is None:
            raise AttributeError()
        key = self.get_indexing_storage_key(
            kb=kb,
            logical_shard=logical_shard,
            resource_uid=message.resource.uuid,
            txid=reindex_id,
        )
        message_serialized = message.SerializeToString()
        logger.debug("Starting to upload bytes")
        await self.uploadbytes(self.indexing_bucket, key, message_serialized)
        logger.debug("Finished to upload bytes")
        return key

    async def get_indexing(self, payload: IndexMessage) -> BrainResource:
        if self.indexing_bucket is None:
            raise AttributeError()
        if payload.storage_key:
            key = payload.storage_key
        else:
            # b/w compatibility
            if payload.txid == 0:
                key = OLD_INDEXING_KEY.format(
                    node=payload.node,
                    shard=payload.shard,
                    txid=payload.reindex_id,
                )
            else:
                key = OLD_INDEXING_KEY.format(
                    node=payload.node, shard=payload.shard, txid=payload.txid
                )

        bytes_buffer = await self.downloadbytes(self.indexing_bucket, key)
        if bytes_buffer.getbuffer().nbytes == 0:
            raise IndexDataNotFound(f'Indexing data not found for key "{key}"')
        pb = BrainResource()
        pb.ParseFromString(bytes_buffer.read())
        bytes_buffer.flush()
        return pb

    async def delete_indexing(
        self,
        resource_uid: str,
        txid: int,
        kb: str,
        logical_shard: str,
    ):
        if self.indexing_bucket is None:
            raise AttributeError()

        # write out empty data but use the .deleted suffix
        # so we know by the key that it was deleted
        key = (
            self.get_indexing_storage_key(
                kb=kb,
                logical_shard=logical_shard,
                resource_uid=resource_uid,
                txid=txid,
            )
            + ".deleted"
        )

        await self.uploadbytes(self.indexing_bucket, key, b"")

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

    async def normalize_binary(
        self, file: CloudFile, destination: StorageField
    ):  # pragma: no cover
        if file.source == self.source and file.uri != destination.key:
            # This MAY BE the case for NucliaDB hosted deployment (Nuclia's cloud deployment):
            # The data has been pushed to the bucket but with a different key.
            logger.warning(
                f"[Nuclia hosted] Source and destination keys differ!: {file.uri} != {destination.key}"
            )
            await self.move(file, destination)
            new_cf = CloudFile()
            new_cf.CopyFrom(file)
            new_cf.bucket_name = destination.bucket
            new_cf.uri = destination.key
        elif file.source == self.source:
            # This is the case for NucliaDB hosted deployment (Nuclia's cloud deployment):
            # The data is already stored in the right place by the processing
            logger.debug(f"[Nuclia hosted]")
            return file
        elif file.source == CloudFile.EXPORT:
            # This is for files coming from an export
            logger.debug(f"[Exported file]: {file.uri}")
            new_cf = CloudFile()
            new_cf.CopyFrom(file)
            new_cf.bucket_name = destination.bucket
            new_cf.uri = destination.key
            new_cf.source = self.source  # type: ignore
        elif file.source == CloudFile.FLAPS:
            # NucliaDB On-Prem: the data is stored in NUA, so we need to
            # download it and upload it to NucliaDB's storage
            logger.debug(f"[NucliaDB OnPrem]: {file.uri}")
            flaps_storage = await get_nuclia_storage()
            iterator = flaps_storage.download(file)
            new_cf = await self.uploaditerator(iterator, destination, file)
        elif file.source == CloudFile.LOCAL:
            # For testing purposes: protobuffer is stored in a file in the local filesystem
            logger.debug(f"[Local]: {file.uri}")
            local_storage = get_local_storage()
            iterator = local_storage.download(file.bucket_name, file.uri)
            new_cf = await self.uploaditerator(iterator, destination, file)
        elif file.source == CloudFile.EMPTY:
            logger.warning(f"[Empty file]: {file.uri}")
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

    async def uploadbytes(
        self,
        bucket: str,
        key: str,
        payload: bytes,
        filename: str = "payload",
        content_type: str = "",
    ):
        destination = self.field_klass(storage=self, bucket=bucket, fullkey=key)

        cf = CloudFile()
        cf.filename = filename
        cf.size = len(payload)
        cf.content_type = content_type
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
        safe_iterator = iterate_storage_compatible(iterator, self, origin)  # type: ignore
        return await destination.upload(safe_iterator, origin)

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

    async def downloadbytescf(self, cf: CloudFile) -> BytesIO:  # pragma: no cover
        # this is covered by other tests
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
    ) -> AsyncGenerator[bytes, None]:  # pragma: no cover
        # this is covered by other tests
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

    @abc.abstractmethod
    async def delete_upload(self, uri: str, bucket_name: str): ...

    @abc.abstractmethod
    def get_bucket_name(self, kbid: str) -> str: ...

    async def initialize(self, service_name: str) -> None:
        await setup_telemetry(service_name)
        await self.raw_driver.initialize()
        for bucket in [self.deadletter_bucket, self.indexing_bucket]:
            if not await self.raw_driver.bucket_exists(bucket):
                await self.raw_driver.create_bucket(bucket)

    async def finalize(self) -> None:
        await self.raw_driver.finalize()

    @abc.abstractmethod
    def iterate_objects(
        self, bucket: str, prefix: str
    ) -> AsyncGenerator[ObjectInfo, None]:
        raise NotImplementedError()
        yield ObjectInfo(name="")

    async def copy(self, file: CloudFile, destination: StorageField) -> None:
        await destination.copy(
            file.uri, destination.key, file.bucket_name, destination.bucket
        )

    async def move(self, file: CloudFile, destination: StorageField) -> None:
        await destination.move(
            file.uri, destination.key, file.bucket_name, destination.bucket
        )

    async def create_kb(self, kbid: str) -> bool:
        bucket_name = self.raw_driver.get_bucket_name(kbid=kbid)
        if await self.raw_driver.bucket_exists(bucket_name):
            return False
        try:
            await self.raw_driver.create_bucket(bucket_name, labels={"kbid": kbid})
            return True
        except Exception as e:
            logger.exception(f"Could not create bucket {kbid}", exc_info=e)
            return False

    async def delete_kb(self, kbid: str) -> tuple[bool, bool]:
        bucket = self.raw_driver.get_bucket_name(kbid=kbid)
        return await self.raw_driver.delete_bucket(bucket)

    async def schedule_delete_kb(self, kbid: str) -> bool:
        bucket = self.get_bucket_name(kbid)
        if not await self.raw_driver.bucket_exists(bucket):
            return False
        await self.raw_driver.schedule_delete_bucket(bucket)
        return True

    async def set_stream_message(self, kbid: str, rid: str, data: bytes) -> str:
        key = MESSAGE_KEY.format(kbid=kbid, rid=rid, mid=uuid.uuid4())
        await self.uploadbytes(cast(str, self.indexing_bucket), key, data)
        return key

    async def get_stream_message(self, key: str) -> bytes:
        bytes_buffer = await self.downloadbytes(cast(str, self.indexing_bucket), key)
        if bytes_buffer.getbuffer().nbytes == 0:
            raise KeyError(f'Stream message data not found for key "{key}"')
        return bytes_buffer.read()

    async def del_stream_message(self, key: str) -> None:
        await self.delete_upload(key, cast(str, self.indexing_bucket))


class RawStorage(abc.ABC, metaclass=abc.ABCMeta):
    source: int
    chunk_size = DEFAULT_CHUNK_SIZE

    @abc.abstractmethod
    async def initialize(self) -> None: ...

    @abc.abstractmethod
    async def finalize(self) -> None: ...

    @abc.abstractmethod
    def get_bucket_name(self, **kwargs) -> str: ...

    @abc.abstractmethod
    async def create_bucket(self, bucket: str, labels: Optional[dict[str, str]] = None) -> None: ...

    @abc.abstractmethod
    async def bucket_exists(self, bucket: str) -> bool: ...

    @abc.abstractmethod
    async def delete_bucket(self, bucket: str) -> None: ...

    @abc.abstractmethod
    async def schedule_delete_bucket(self, bucket: str) -> None: ...

    @abc.abstractmethod
    async def move_object(self, origin_bucket: str, origin_key: str, destination_bucket: str, destination_key: str) -> None: ...

    @abc.abstractmethod
    async def copy_object(self, origin_bucket: str, origin_key: str, destination_bucket: str, destination_key: str) -> None: ...

    @abc.abstractmethod
    async def delete_object(self, bucket: str, key: str) -> None: ...

    @abc.abstractmethod
    async def upload_object(self, bucket: str, key: str, data: Union[bytes, AsyncGenerator[bytes, None]]) -> None: ...

    @abc.abstractmethod
    async def download_object(self, bucket: str, key: str) -> bytes: ...

    @abc.abstractmethod
    async def download_object_stream(self, bucket: str, key: str) -> AsyncGenerator[bytes, None]:
        raise NotImplementedError()
        yield b""

    @abc.abstractmethod
    async def iter_objects(self, bucket: str, prefix: str) -> AsyncGenerator[ObjectInfo, None]:
        raise NotImplementedError()
        yield ObjectInfo(name="")
    
    @abc.abstractmethod
    async def get_object_metadata(self, bucket: str, key: str) -> ObjectMetadata: ...

    @abc.abstractmethod
    async def multipart_upload_start(self, bucket: str, key: str, metadata: ObjectMetadata) -> str: ...

    @abc.abstractmethod
    async def multipart_upload_append(self, bucket: str, key: str, iterable: AsyncIterator[bytes]): ...

    @abc.abstractmethod
    async def multipart_upload_finish(self, bucket: str, key: str) -> None: ...




async def iter_and_add_size(
    stream: AsyncGenerator[bytes, None], cf: CloudFile
) -> AsyncGenerator[bytes, None]:
    # This is needed because some storage types like GCS or S3 require
    # the size of the file at least at the request done for the last chunk.
    total_size = 0
    async for chunk, is_last in async_gen_lookahead(stream):
        total_size += len(chunk)
        if is_last:
            cf.size = total_size
        yield chunk


async def iter_in_chunk_size(
    iterator: AsyncGenerator[bytes, None], chunk_size: int
) -> AsyncGenerator[bytes, None]:
    # This is needed to make sure bytes uploaded to the blob storage complies with a particular chunk size.
    buffer = b""
    async for chunk in iterator:
        buffer += chunk
        if len(buffer) >= chunk_size:
            yield buffer[:chunk_size]
            buffer = buffer[chunk_size:]
    # The last chunk can be smaller than chunk size
    if len(buffer) > 0:
        yield buffer


async def iterate_storage_compatible(
    iterator: AsyncGenerator[bytes, None], storage: Storage, cf: CloudFile
) -> AsyncGenerator[bytes, None]:
    """
    Makes sure to add the size to the cloudfile and split the data in
    chunks that are compatible with the storage type of choice
    """

    async for chunk in iter_in_chunk_size(
        iter_and_add_size(iterator, cf), chunk_size=storage.chunk_size
    ):
        yield chunk
