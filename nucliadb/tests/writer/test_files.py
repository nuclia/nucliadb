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
import base64
import io
import os

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX
from nucliadb.writer.api.v1.upload import maybe_b64decode
from nucliadb.writer.tus import TUSUPLOAD, UPLOAD, get_storage_manager
from nucliadb_protos.resources_pb2 import FieldID, FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils import const
from nucliadb_utils.utilities import get_storage, get_transaction_utility

ASSETS_PATH = os.path.dirname(__file__) + "/assets"


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_file_tus_options(nucliadb_writer: AsyncClient, knowledgebox: str):
    kbid = knowledgebox

    resp = await nucliadb_writer.options(f"/{KB_PREFIX}/{kbid}/resource/xxx/file/xxx/{TUSUPLOAD}/xxx")
    assert resp.status_code == 204
    assert resp.headers["tus-resumable"] == "1.0.0"
    assert resp.headers["tus-version"] == "1.0.0"
    assert resp.headers["tus-extension"] == "creation-defer-length"

    resp = await nucliadb_writer.options(f"/{KB_PREFIX}/{kbid}/resource/xxx/file/xxx/{TUSUPLOAD}")
    assert resp.status_code == 204
    assert resp.headers["tus-resumable"] == "1.0.0"
    assert resp.headers["tus-version"] == "1.0.0"
    assert resp.headers["tus-extension"] == "creation-defer-length"

    resp = await nucliadb_writer.options(f"/{KB_PREFIX}/{kbid}/{TUSUPLOAD}")
    assert resp.status_code == 204
    assert resp.headers["tus-resumable"] == "1.0.0"
    assert resp.headers["tus-version"] == "1.0.0"
    assert resp.headers["tus-extension"] == "creation-defer-length"

    resp = await nucliadb_writer.options(f"/{KB_PREFIX}/{kbid}/{TUSUPLOAD}/xxx")
    assert resp.status_code == 204
    assert resp.headers["tus-resumable"] == "1.0.0"
    assert resp.headers["tus-version"] == "1.0.0"
    assert resp.headers["tus-extension"] == "creation-defer-length"


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_file_tus_upload_root(nucliadb_writer: AsyncClient, knowledgebox: str):
    kbid = knowledgebox

    language = base64.b64encode(b"ca").decode()
    filename = base64.b64encode(b"image.jpg").decode()
    md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{TUSUPLOAD}",
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": f"filename {filename},language {language},md5 {md5}",
            "content-type": "image/jpeg",
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 201
    url = resp.headers["location"]

    offset = 0

    # We upload a file that spans across more than one chunk
    min_chunk_size = get_storage_manager().min_upload_size
    assert min_chunk_size is not None, "File storage not properly set up"
    raw_bytes = b"x" * min_chunk_size + b"y" * 500
    io_bytes = io.BytesIO(raw_bytes)
    data = io_bytes.read(min_chunk_size)
    while data != b"":
        resp = await nucliadb_writer.head(url)
        assert resp.headers["Upload-Length"] == f"0"
        assert resp.headers["Upload-Offset"] == f"{offset}"

        headers = {
            "upload-offset": f"{offset}",
            "content-length": f"{len(data)}",
        }
        is_last_chunk = len(data) < min_chunk_size
        if is_last_chunk:
            headers["upload-length"] = f"{offset + len(data)}"

        resp = await nucliadb_writer.patch(
            url,
            content=data,
            headers=headers,
        )
        offset += len(data)
        data = io_bytes.read(min_chunk_size)

    assert resp.headers["Tus-Upload-Finished"] == "1"

    transaction = get_transaction_utility()

    sub = await transaction.js.pull_subscribe(const.Streams.INGEST.subject.format(partition="1"), "auto")
    msgs = await sub.fetch(1)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[0].data)
    await msgs[0].ack()

    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpeg"
    assert writer.basic.title == "image.jpg"
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == len(raw_bytes)
    assert writer.files[field].file.filename == "image.jpg"
    assert writer.files[field].file.md5 == "7af0916dba8b70e29d99e72941923529"

    storage = await get_storage()
    download_data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(download_data.read()) == len(raw_bytes)
    await asyncio.sleep(1)

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{TUSUPLOAD}",
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": f"filename {filename},language {language},md5 {md5}",
            "content-type": "image/jpeg",
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 409


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_file_upload_root(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox

    with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{kbid}/{UPLOAD}",
            content=f.read(),
            headers={
                "content-type": "image/jpeg",
                "X-MD5": "7af0916dba8b70e29d99e72941923529",
            },
        )
        assert resp.status_code == 201

    transaction = get_transaction_utility()

    assert transaction.js is not None
    sub = await transaction.js.pull_subscribe(const.Streams.INGEST.subject.format(partition="1"), "auto")
    msgs = await sub.fetch(1)
    writer = BrokerMessage()
    writer.ParseFromString(msgs[0].data)
    await msgs[0].ack()

    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpeg"
    assert writer.files[field].file.size == 30472

    storage = await get_storage()
    download_data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(download_data.read()) == 30472
    await asyncio.sleep(1)

    with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{kbid}/{UPLOAD}",
            content=f.read(),
            headers={
                "content-type": "image/jpeg",
                "X-MD5": "7af0916dba8b70e29d99e72941923529",
            },
        )
        assert resp.status_code == 409


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_file_upload_root_headers(
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox

    filename = base64.b64encode(b"image.jpg").decode()
    with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{kbid}/{UPLOAD}",
            content=f.read(),
            headers={
                "X-FILENAME": filename,
                "X-LANGUAGE": "ca",
                "X-MD5": "7af0916dba8b70e29d99e72941923529",
                "content-type": "image/jpeg",
            },
        )
        assert resp.status_code == 201

    transaction = get_transaction_utility()

    assert transaction.js is not None
    sub = await transaction.js.pull_subscribe(const.Streams.INGEST.subject.format(partition="1"), "auto")
    msgs = await sub.fetch(1)
    writer = BrokerMessage()
    writer.ParseFromString(msgs[0].data)
    await msgs[0].ack()

    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpeg"
    assert writer.basic.title == "image.jpg"
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == 30472

    storage = await get_storage()
    download_data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(download_data.read()) == 30472


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_file_tus_upload_field(
    nucliadb_writer: AsyncClient, knowledgebox: str, resource
):
    kbid = knowledgebox

    language = base64.b64encode(b"ca").decode()
    filename = base64.b64encode(b"image.jpg").decode()
    md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resource/invalidresource/file/field1/{TUSUPLOAD}",
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": f"filename {filename},language {language},md5 {md5}",
            "content-type": "image/jpeg",
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 404
    await asyncio.sleep(1)

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resource/{resource}/file/field1/{TUSUPLOAD}",
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": f"filename {filename},language {language},md5 {md5}",
            "content-type": "image/jpeg",
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 201
    url = resp.headers["location"]

    offset = 0
    # We upload a file that spans across more than one chunk
    min_chunk_size = get_storage_manager().min_upload_size
    assert min_chunk_size is not None, "File storage not properly set up"
    raw_bytes = b"x" * min_chunk_size + b"y" * 500
    io_bytes = io.BytesIO(raw_bytes)
    data = io_bytes.read(min_chunk_size)
    while data != b"":
        resp = await nucliadb_writer.head(url)

        assert resp.headers["Upload-Length"] == f"0"
        assert resp.headers["Upload-Offset"] == f"{offset}"

        headers = {
            "upload-offset": f"{offset}",
            "content-length": f"{len(data)}",
        }
        is_last_chunk = len(data) < min_chunk_size
        if is_last_chunk:
            headers["upload-length"] = f"{offset + len(data)}"

        resp = await nucliadb_writer.patch(
            url,
            content=data,
            headers=headers,
        )
        assert resp.status_code == 200
        offset += len(data)
        data = io_bytes.read(min_chunk_size)

    assert resp.headers["Tus-Upload-Finished"] == "1"

    transaction = get_transaction_utility()

    sub = await transaction.js.pull_subscribe(const.Streams.INGEST.subject.format(partition="1"), "auto")
    msgs = await sub.fetch(2)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[1].data)
    await msgs[1].ack()

    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpeg"
    assert writer.basic.title == ""
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == len(raw_bytes)
    assert writer.files[field].file.filename == "image.jpg"
    assert writer.files[field].file.md5 == "7af0916dba8b70e29d99e72941923529"

    storage = await get_storage()
    download_data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(download_data.read()) == len(raw_bytes)


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_file_upload_field_headers(
    nucliadb_writer: AsyncClient, knowledgebox: str, resource
):
    kbid = knowledgebox
    filename = "image.jpg"
    encoded_filename = base64.b64encode(filename.encode()).decode()
    with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{kbid}/resource/{resource}/file/field1/{UPLOAD}",
            content=f.read(),
            headers={
                "X-FILENAME": encoded_filename,
                "X-LANGUAGE": "ca",
                "X-MD5": "7af0916dba8b70e29d99e72941923529",
                "content-type": "image/jpeg",
            },
        )
        assert resp.status_code == 201

    transaction = get_transaction_utility()

    sub = await transaction.js.pull_subscribe(const.Streams.INGEST.subject.format(partition="1"), "auto")
    msgs = await sub.fetch(2)
    writer = BrokerMessage()
    writer.ParseFromString(msgs[1].data)
    await msgs[1].ack()

    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpeg"
    assert writer.basic.title == ""
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == 30472
    assert writer.files[field].file.filename == filename

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472


@pytest.mark.deploy_modes("component")
async def test_knowledgebox_file_upload_field_sync(
    nucliadb_writer: AsyncClient, knowledgebox: str, resource
):
    kbid = knowledgebox
    filename = "image.jpg"
    with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{kbid}/resource/{resource}/file/field1/{UPLOAD}",
            content=f.read(),
            headers={
                "X-FILENAME": filename,
                "X-LANGUAGE": "ca",
                "X-MD5": "7af0916dba8b70e29d99e72941923529",
                "content-type": "image/jpeg",
            },
        )
        assert resp.status_code == 201

    async with datamanagers.with_ro_transaction() as txn:
        assert (
            await datamanagers.resources.has_field(
                txn,
                kbid=kbid,
                rid=resource,
                field_id=FieldID(field="field1", field_type=FieldType.FILE),
            )
        ) is True


@pytest.mark.deploy_modes("component")
async def test_file_tus_upload_field_by_slug(nucliadb_writer: AsyncClient, knowledgebox: str, resource):
    kbid = knowledgebox
    rslug = "resource1"

    language = base64.b64encode(b"ca").decode()
    filename = base64.b64encode(b"image.jpg").decode()
    md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()
    headers = {
        "tus-resumable": "1.0.0",
        "upload-metadata": f"filename {filename},language {language},md5 {md5}",
        "content-type": "image/jpeg",
        "upload-defer-length": "1",
    }

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/slug/idonotexist/file/field1/{TUSUPLOAD}",
        headers=headers,
    )
    assert resp.status_code == 404

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/slug/{rslug}/file/field1/{TUSUPLOAD}",
        headers=headers,
    )
    assert resp.status_code == 201
    url = resp.headers["location"]

    # Check that we are using the slug for the whole file upload
    assert f"{RSLUG_PREFIX}/{rslug}" in url

    offset = 0
    min_chunk_size = get_storage_manager().min_upload_size
    assert min_chunk_size is not None, "File storage not properly set up"
    raw_bytes = b"x" * min_chunk_size + b"y" * 500
    io_bytes = io.BytesIO(raw_bytes)
    data = io_bytes.read(min_chunk_size)
    while data != b"":
        resp = await nucliadb_writer.head(url)

        assert resp.headers["Upload-Length"] == f"0"
        assert resp.headers["Upload-Offset"] == f"{offset}"

        headers = {
            "upload-offset": f"{offset}",
            "content-length": f"{len(data)}",
        }
        is_last_chunk = len(data) < min_chunk_size
        if is_last_chunk:
            headers["upload-length"] = f"{offset + len(data)}"

        resp = await nucliadb_writer.patch(
            url,
            content=data,
            headers=headers,
        )
        assert resp.status_code == 200
        offset += len(data)
        data = io_bytes.read(min_chunk_size)

    assert resp.headers["Tus-Upload-Finished"] == "1"

    transaction = get_transaction_utility()

    sub = await transaction.js.pull_subscribe(const.Streams.INGEST.subject.format(partition="1"), "auto")
    msgs = await sub.fetch(2)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[1].data)
    await msgs[1].ack()

    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpeg"
    assert writer.basic.title == ""
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == len(raw_bytes)
    assert writer.files[field].file.filename == "image.jpg"
    assert writer.files[field].file.md5 == "7af0916dba8b70e29d99e72941923529"

    storage = await get_storage()
    download_data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(download_data.read()) == len(raw_bytes)


@pytest.mark.deploy_modes("component")
async def test_file_tus_upload_urls_field_by_resource_id(
    nucliadb_writer: AsyncClient, knowledgebox: str, resource
):
    kbid = knowledgebox
    language = base64.b64encode(b"ca").decode()
    filename = base64.b64encode(b"image.jpg").decode()
    md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()
    headers = {
        "tus-resumable": "1.0.0",
        "upload-metadata": f"filename {filename},language {language},md5 {md5}",
        "content-type": "image/jpeg",
        "upload-defer-length": "1",
    }

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resource/idonotexist/file/field1/{TUSUPLOAD}",
        headers=headers,
    )
    assert resp.status_code == 404

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resource/{resource}/file/field1/{TUSUPLOAD}",
        headers=headers,
    )
    assert resp.status_code == 201
    url = resp.headers["location"]

    # Check that we are using the resource for the whole file upload
    assert f"{RESOURCE_PREFIX}/{resource}" in url

    # Make sure the returned URL works
    resp = await nucliadb_writer.head(url)
    assert resp.status_code == 200

    assert resp.headers["Upload-Length"] == "0"
    assert resp.headers["Upload-Offset"] == "0"


@pytest.mark.deploy_modes("component")
async def test_multiple_tus_file_upload_tries(nucliadb_writer: AsyncClient, knowledgebox: str, resource):
    kbid = knowledgebox
    rslug = "resource1"

    headers = {
        "tus-resumable": "1.0.0",
        "content-type": "image/jpeg",
        "upload-defer-length": "1",
    }

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/slug/{rslug}/file/field1/{TUSUPLOAD}",
        headers=headers,
    )
    assert resp.status_code == 201
    url = resp.headers["location"]

    # Check that we are using the slug for the whole file upload
    assert f"{RSLUG_PREFIX}/{rslug}" in url
    resp = await nucliadb_writer.patch(
        url,
        content=b"x" * 10000,
        headers={
            "upload-offset": "0",
            "content-length": "10000",
            "upload-length": "10000",
        },
    )
    assert resp.status_code == 200

    assert resp.headers["Tus-Upload-Finished"] == "1"

    # next one should work as well
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/slug/{rslug}/file/field1/{TUSUPLOAD}",
        headers=headers,
    )
    assert resp.status_code == 201
    url = resp.headers["location"]

    # Check that we are using the slug for the whole file upload
    assert f"{RSLUG_PREFIX}/{rslug}" in url
    resp = await nucliadb_writer.patch(
        url,
        content=b"x" * 10000,
        headers={
            "upload-offset": "0",
            "content-length": "10000",
            "upload-length": "10000",
        },
    )
    assert resp.status_code == 200

    assert resp.headers["Tus-Upload-Finished"] == "1"


@pytest.mark.deploy_modes("component")
async def test_file_upload_by_slug(nucliadb_writer: AsyncClient, knowledgebox: str):
    kbid = knowledgebox
    rslug = "myslug"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/resources",
        json={
            "slug": rslug,
        },
    )
    assert str(resp.status_code).startswith("2")

    filename = "image.jpg"
    with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{kbid}/{RSLUG_PREFIX}/{rslug}/file/file1/{UPLOAD}",
            content=f.read(),
            headers={
                "X-FILENAME": filename,
                "content-type": "image/jpeg",
                "X-MD5": "7af0916dba8b70e29d99e72941923529",
            },
        )
        assert resp.status_code == 201

    transaction = get_transaction_utility()

    sub = await transaction.js.pull_subscribe(const.Streams.INGEST.subject.format(partition="1"), "auto")
    msgs = await sub.fetch(2)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[-1].data)
    await msgs[-1].ack()

    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]

    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpeg"
    assert writer.files[field].file.size == 30472
    assert writer.files[field].file.filename == filename

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472


def test_maybe_b64decode():
    something = "something"
    something_encoded = base64.b64encode(something.encode())
    assert maybe_b64decode(something_encoded) == something
    assert maybe_b64decode(something) == something


@pytest.mark.deploy_modes("component")
async def test_tus_validates_intermediate_chunks_length(nucliadb_writer: AsyncClient, knowledgebox: str):
    kbid = knowledgebox
    language = base64.b64encode(b"ca").decode()
    filename = base64.b64encode(b"image.jpg").decode()
    md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{kbid}/{TUSUPLOAD}",
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": f"filename {filename},language {language},md5 {md5}",
            "content-type": "image/jpeg",
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 201
    url = resp.headers["location"]
    # We upload a chunk that is smaller than the minimum chunk size
    min_chunk_size = get_storage_manager().min_upload_size
    assert min_chunk_size is not None, "File storage not properly set up"
    raw_bytes = b"x" * min_chunk_size + b"y" * 500
    io_bytes = io.BytesIO(raw_bytes)
    chunk = io_bytes.read(min_chunk_size - 10)

    resp = await nucliadb_writer.head(url)

    headers = {
        "upload-offset": f"0",
        "content-length": f"{len(chunk)}",
    }
    resp = await nucliadb_writer.patch(
        url,
        content=chunk,
        headers=headers,
    )
    assert resp.status_code == 412
    assert "Intermediate chunks" in resp.json()["detail"]
