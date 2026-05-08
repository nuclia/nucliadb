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

"""OneFS S3-compatibility tests

This test suite has been adapted from the one for backend for the things
applying to NucliaDB.

"""

from unittest.mock import AsyncMock

import botocore.exceptions
import pytest

from nucliadb_utils.storages.s3 import S3Storage, S3StorageField


def make_client_error(code, status_code=400, message="mocked error"):
    return botocore.exceptions.ClientError(
        {
            "Error": {"Code": str(code), "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status_code},
        },
        "TestOperation",
    )


def make_storage(**kwargs) -> S3Storage:
    storage = S3Storage.__new__(S3Storage)
    storage.bucket = "bucket"
    storage.source = None
    storage._aws_access_key = None
    storage._aws_secret_key = None
    storage._region_name = "myregion"
    storage._kms_key_id = None
    storage._signed_url_base = kwargs.get("signed_url_base")
    storage._bucket_tags = kwargs.get("bucket_tags")
    storage._normalize_binary_content_type = kwargs.get("normalize_binary_content_type", False)
    storage.opts = {"region_name": None}
    storage._s3aioclient = AsyncMock()
    return storage


# --- create_bucket: graceful tagging failure ---


async def test_create_bucket_without_tags_does_not_call_tagging():
    storage = make_storage()
    storage._s3aioclient.head_bucket.side_effect = make_client_error("404", 404)
    storage._s3aioclient.create_bucket = AsyncMock()  # type: ignore[ty:unresolved-attribute]
    storage._s3aioclient.put_bucket_tagging.side_effect = NotImplementedError

    await storage.create_bucket("mybucket")

    assert storage._s3aioclient.create_bucket.call_count == 1
    assert storage._s3aioclient.put_bucket_tagging.call_count == 0


async def test_create_bucket_without_kms_keys_does_not_encrypt():
    storage = make_storage()
    storage._s3aioclient.head_bucket.side_effect = make_client_error("404", 404)
    storage._s3aioclient.create_bucket = AsyncMock()  # type: ignore[ty:unresolved-attribute]
    storage._s3aioclient.put_bucket_encryption.side_effect = NotImplementedError

    await storage.create_bucket("mybucket")

    assert storage._s3aioclient.create_bucket.call_count == 1
    assert storage._s3aioclient.put_bucket_encryption.call_count == 0


# --- delete_bucket: string error codes ---


async def test_delete_bucket_conflict_string_code():
    storage = make_storage()
    storage._s3aioclient.head_bucket = AsyncMock(  # type: ignore[ty:unresolved-attribute]
        return_value={"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    storage._s3aioclient.delete_bucket.side_effect = make_client_error("BucketNotEmpty", 409)

    deleted, conflict = await storage.delete_kb("mykb")

    assert deleted is False
    assert conflict is True


async def test_delete_bucket_conflict_numeric_string_code():
    storage = make_storage()
    storage._s3aioclient.head_bucket = AsyncMock(  # type: ignore[ty:unresolved-attribute]
        return_value={"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    storage._s3aioclient.delete_bucket.side_effect = make_client_error("409", 409)

    deleted, conflict = await storage.delete_kb("mykb")

    assert deleted is False
    assert conflict is True


async def test_delete_bucket_nosuchbucket():
    storage = make_storage()
    storage._s3aioclient.head_bucket = AsyncMock(  # type: ignore[ty:unresolved-attribute]
        return_value={"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    storage._s3aioclient.delete_bucket.side_effect = make_client_error("NoSuchBucket", 404)

    deleted, conflict = await storage.delete_kb("mykb")

    assert deleted is False
    assert conflict is False


# --- exists: error code handling ---


async def test_exists_handles_404_string_code():
    storage = make_storage()
    storage._s3aioclient.head_object.side_effect = make_client_error("404", 404)

    field = S3StorageField(storage=storage, bucket="b", fullkey="k")
    result = await field.exists()
    assert result is None


async def test_exists_handles_nosuchkey_code():
    storage = make_storage()
    storage._s3aioclient.head_object.side_effect = make_client_error("NoSuchKey", 404)

    field = S3StorageField(storage=storage, bucket="b", fullkey="k")
    result = await field.exists()
    assert result is None


async def test_exists_raises_on_other_errors():
    storage = make_storage()
    storage._s3aioclient.head_object.side_effect = make_client_error("AccessDenied", 403)

    field = S3StorageField(storage=storage, bucket="b", fullkey="k")
    with pytest.raises(botocore.exceptions.ClientError):
        await field.exists()


# --- normalize binary content type ---


async def test_normalize_binary_content_type():
    storage = make_storage()
    storage._s3aioclient.head_object = AsyncMock(  # type: ignore[ty:unresolved-attribute]
        return_value={
            "Metadata": {},
            "ContentLength": 100,
            "ContentType": "binary/octet-stream",
            "ETag": '"abc123"',
        }
    )

    field = S3StorageField(storage=storage, bucket="b", fullkey="k")
    meta = await field.exists()

    assert meta is not None
    assert meta.content_type == "application/octet-stream"


async def test_normalize_binary_content_type_does_not_change_other_types():
    storage = make_storage(normalize_binary_content_type=True)
    storage._s3aioclient.head_object = AsyncMock(  # type: ignore[ty:unresolved-attribute]
        return_value={
            "Metadata": {},
            "ContentLength": 50,
            "ContentType": "text/plain",
            "ETag": '"def456"',
        }
    )

    field = S3StorageField(storage=storage, bucket="b", fullkey="k")
    meta = await field.exists()

    assert meta is not None
    assert meta.content_type == "text/plain"
