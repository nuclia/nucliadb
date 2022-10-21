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
import os

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage, ResourceFieldId

from nucliadb.models.resource import NucliaDBRoles
from nucliadb.writer.api.v1.router import KB_PREFIX, RSLUG_PREFIX
from nucliadb.writer.api.v1.upload import maybe_b64decode
from nucliadb.writer.tus import TUSUPLOAD, UPLOAD
from nucliadb.writer.utilities import get_processing
from nucliadb_utils.utilities import get_ingest, get_storage, get_transaction

ASSETS_PATH = os.path.dirname(__file__) + "/assets"


@pytest.mark.asyncio
async def test_knowledgebox_file_tus_options(writer_api, knowledgebox_writer):
    client: AsyncClient
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.options(
            f"/{KB_PREFIX}/{knowledgebox_writer}/resource/xxx/file/xxx/{TUSUPLOAD}/xxx"
        )
        assert resp.status_code == 204
        assert resp.headers["tus-resumable"] == "1.0.0"
        assert resp.headers["tus-version"] == "1.0.0"
        assert resp.headers["tus-extension"] == "creation-defer-length"

        resp = await client.options(
            f"/{KB_PREFIX}/{knowledgebox_writer}/resource/xxx/file/xxx/{TUSUPLOAD}"
        )
        assert resp.status_code == 204
        assert resp.headers["tus-resumable"] == "1.0.0"
        assert resp.headers["tus-version"] == "1.0.0"
        assert resp.headers["tus-extension"] == "creation-defer-length"

        resp = await client.options(f"/{KB_PREFIX}/{knowledgebox_writer}/{TUSUPLOAD}")
        assert resp.status_code == 204
        assert resp.headers["tus-resumable"] == "1.0.0"
        assert resp.headers["tus-version"] == "1.0.0"
        assert resp.headers["tus-extension"] == "creation-defer-length"

        resp = await client.options(
            f"/{KB_PREFIX}/{knowledgebox_writer}/{TUSUPLOAD}/xxx"
        )
        assert resp.status_code == 204
        assert resp.headers["tus-resumable"] == "1.0.0"
        assert resp.headers["tus-version"] == "1.0.0"
        assert resp.headers["tus-extension"] == "creation-defer-length"


@pytest.mark.asyncio
async def test_knowledgebox_file_tus_upload_root(writer_api, knowledgebox_writer):
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        language = base64.b64encode(b"ca").decode()
        filename = base64.b64encode(b"image.jpg").decode()
        md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_writer}/{TUSUPLOAD}",
            headers={
                "tus-resumable": "1.0.0",
                "upload-metadata": f"filename {filename},language {language},md5 {md5}",
                "content-type": "image/jpg",
                "upload-defer-length": "1",
            },
        )
        assert resp.status_code == 201
        url = resp.headers["location"]

        offset = 0
        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            data = f.read(10000)
            while data != b"":
                resp = await client.head(
                    url,
                )

                assert resp.headers["Upload-Length"] == f"0"
                assert resp.headers["Upload-Offset"] == f"{offset}"

                headers = {
                    "upload-offset": f"{offset}",
                    "content-length": f"{len(data)}",
                }
                if len(data) < 10000:
                    headers["upload-length"] = f"{offset + len(data)}"

                resp = await client.patch(
                    url,
                    data=data,
                    headers=headers,
                )
                offset += len(data)
                data = f.read(10000)

        assert resp.headers["Tus-Upload-Finished"] == "1"

    processing = get_processing()
    transaction = get_transaction()

    sub = await transaction.js.pull_subscribe("nucliadb.1", "auto")
    msgs = await sub.fetch(1)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[0].data)
    await msgs[0].ack()

    payload = processing.calls[0]

    assert payload["kbid"] == knowledgebox_writer
    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
    assert payload["filefield"][field] == "DUMMYJWT"
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpg"
    assert writer.basic.title == "image.jpg"
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == 30472
    assert writer.files[field].file.filename == "image.jpg"
    assert writer.files[field].file.md5 == "7af0916dba8b70e29d99e72941923529"

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472
    await asyncio.sleep(1)

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_writer}/{TUSUPLOAD}",
            headers={
                "tus-resumable": "1.0.0",
                "upload-metadata": f"filename {filename},language {language},md5 {md5}",
                "content-type": "image/jpg",
                "upload-defer-length": "1",
            },
        )
        assert resp.status_code == 409


@pytest.mark.asyncio
async def test_knowledgebox_file_upload_root(writer_api, knowledgebox_writer):
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            resp = await client.post(
                f"/{KB_PREFIX}/{knowledgebox_writer}/{UPLOAD}",
                data=f.read(),
                headers={
                    "content-type": "image/jpg",
                    "X-MD5": "7af0916dba8b70e29d99e72941923529",
                },
            )
            assert resp.status_code == 201

    processing = get_processing()
    transaction = get_transaction()

    sub = await transaction.js.pull_subscribe("nucliadb.1", "auto")
    msgs = await sub.fetch(1)
    writer = BrokerMessage()
    writer.ParseFromString(msgs[0].data)
    await msgs[0].ack()

    payload = processing.calls[0]

    assert payload["kbid"] == knowledgebox_writer
    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]
    assert payload["filefield"][field] == "DUMMYJWT"
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpg"
    assert writer.files[field].file.size == 30472

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472
    await asyncio.sleep(1)

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            resp = await client.post(
                f"/{KB_PREFIX}/{knowledgebox_writer}/{UPLOAD}",
                data=f.read(),
                headers={
                    "content-type": "image/jpg",
                    "X-MD5": "7af0916dba8b70e29d99e72941923529",
                },
            )
            assert resp.status_code == 409


@pytest.mark.asyncio
async def test_knowledgebox_file_upload_root_headers(writer_api, knowledgebox_writer):
    client: AsyncClient
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        filename = base64.b64encode(b"image.jpg").decode()
        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            resp = await client.post(
                f"/{KB_PREFIX}/{knowledgebox_writer}/{UPLOAD}",
                data=f.read(),
                headers={
                    "X-FILENAME": filename,
                    "X-LANGUAGE": "ca",
                    "X-MD5": "7af0916dba8b70e29d99e72941923529",
                    "content-type": "image/jpg",
                },
            )
            assert resp.status_code == 201

    processing = get_processing()
    transaction = get_transaction()

    sub = await transaction.js.pull_subscribe("nucliadb.1", "auto")
    msgs = await sub.fetch(1)
    writer = BrokerMessage()
    writer.ParseFromString(msgs[0].data)
    await msgs[0].ack()

    payload = processing.calls[0]

    assert payload["kbid"] == knowledgebox_writer
    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]
    assert payload["filefield"][field] == "DUMMYJWT"
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpg"
    assert writer.basic.title == "image.jpg"
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == 30472

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472


@pytest.mark.asyncio
async def test_knowledgebox_file_tus_upload_field(
    writer_api, knowledgebox_writer, resource
):
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        language = base64.b64encode(b"ca").decode()
        filename = base64.b64encode(b"image.jpg").decode()
        md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()

        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_writer}/resource/invalidresource/file/field1/{TUSUPLOAD}",
            headers={
                "tus-resumable": "1.0.0",
                "upload-metadata": f"filename {filename},language {language},md5 {md5}",
                "content-type": "image/jpg",
                "upload-defer-length": "1",
            },
        )
        assert resp.status_code == 404
        await asyncio.sleep(1)

        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_writer}/resource/{resource}/file/field1/{TUSUPLOAD}",
            headers={
                "tus-resumable": "1.0.0",
                "upload-metadata": f"filename {filename},language {language},md5 {md5}",
                "content-type": "image/jpg",
                "upload-defer-length": "1",
            },
        )
        assert resp.status_code == 201
        url = resp.headers["location"]

        offset = 0
        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            data = f.read(10000)
            while data != b"":
                resp = await client.head(
                    url,
                )

                assert resp.headers["Upload-Length"] == f"0"
                assert resp.headers["Upload-Offset"] == f"{offset}"

                headers = {
                    "upload-offset": f"{offset}",
                    "content-length": f"{len(data)}",
                }
                if len(data) < 10000:
                    headers["upload-length"] = f"{offset + len(data)}"

                resp = await client.patch(
                    url,
                    data=data,
                    headers=headers,
                )
                assert resp.status_code == 200
                offset += len(data)
                data = f.read(10000)

        assert resp.headers["Tus-Upload-Finished"] == "1"

    processing = get_processing()
    transaction = get_transaction()

    sub = await transaction.js.pull_subscribe("nucliadb.1", "auto")
    msgs = await sub.fetch(2)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[1].data)
    await msgs[1].ack()

    payload = processing.calls[1]

    assert payload["kbid"] == knowledgebox_writer
    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
    assert payload["filefield"][field] == "DUMMYJWT"
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpg"
    assert writer.basic.title == ""
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == 30472
    assert writer.files[field].file.filename == "image.jpg"
    assert writer.files[field].file.md5 == "7af0916dba8b70e29d99e72941923529"

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472


@pytest.mark.asyncio
async def test_knowledgebox_file_upload_field_headers(
    writer_api, knowledgebox_writer, resource
):
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        filename = "image.jpg"
        encoded_filename = base64.b64encode(filename.encode()).decode()
        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            resp = await client.post(
                f"/{KB_PREFIX}/{knowledgebox_writer}/resource/{resource}/file/field1/{UPLOAD}",
                data=f.read(),
                headers={
                    "X-FILENAME": encoded_filename,
                    "X-LANGUAGE": "ca",
                    "X-MD5": "7af0916dba8b70e29d99e72941923529",
                    "content-type": "image/jpg",
                },
            )
            assert resp.status_code == 201

    processing = get_processing()
    transaction = get_transaction()

    sub = await transaction.js.pull_subscribe("nucliadb.1", "auto")
    msgs = await sub.fetch(2)
    writer = BrokerMessage()
    writer.ParseFromString(msgs[1].data)
    await msgs[1].ack()

    payload = processing.calls[1]

    assert payload["kbid"] == knowledgebox_writer
    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]
    assert payload["filefield"][field] == "DUMMYJWT"
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpg"
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


@pytest.mark.asyncio
async def test_knowledgebox_file_upload_field_sync(
    writer_api, knowledgebox_writer, resource
):
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        filename = "image.jpg"
        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            resp = await client.post(
                f"/{KB_PREFIX}/{knowledgebox_writer}/resource/{resource}/file/field1/{UPLOAD}",
                data=f.read(),
                headers={
                    "X-FILENAME": filename,
                    "X-LANGUAGE": "ca",
                    "X-MD5": "7af0916dba8b70e29d99e72941923529",
                    "content-type": "image/jpg",
                    "X-SYNCHRONOUS": "True",
                },
            )
            assert resp.status_code == 201

        ingest = get_ingest()
        pbrequest = ResourceFieldId()
        pbrequest.kbid = knowledgebox_writer
        pbrequest.rid = resource
        pbrequest.field_type = FieldType.FILE
        pbrequest.field = "field1"

        res = await ingest.ResourceFieldExists(pbrequest)
        assert res.found


@pytest.mark.asyncio
async def test_file_tus_upload_field_by_slug(writer_api, knowledgebox_writer, resource):
    kb = knowledgebox_writer
    rslug = "resource1"

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:

        language = base64.b64encode(b"ca").decode()
        filename = base64.b64encode(b"image.jpg").decode()
        md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()
        headers = {
            "tus-resumable": "1.0.0",
            "upload-metadata": f"filename {filename},language {language},md5 {md5}",
            "content-type": "image/jpg",
            "upload-defer-length": "1",
        }

        resp = await client.post(
            f"/{KB_PREFIX}/{kb}/slug/idonotexist/file/field1/{TUSUPLOAD}",
            headers=headers,
        )
        assert resp.status_code == 404

        resp = await client.post(
            f"/{KB_PREFIX}/{kb}/slug/{rslug}/file/field1/{TUSUPLOAD}",
            headers=headers,
        )
        assert resp.status_code == 201
        url = resp.headers["location"]

        # Check that we are using the slug for the whole file upload
        assert f"{RSLUG_PREFIX}/{rslug}" in url

        offset = 0
        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            data = f.read(10000)
            while data != b"":
                resp = await client.head(
                    url,
                )

                assert resp.headers["Upload-Length"] == f"0"
                assert resp.headers["Upload-Offset"] == f"{offset}"

                headers = {
                    "upload-offset": f"{offset}",
                    "content-length": f"{len(data)}",
                }
                if len(data) < 10000:
                    headers["upload-length"] = f"{offset + len(data)}"

                resp = await client.patch(
                    url,
                    data=data,
                    headers=headers,
                )
                assert resp.status_code == 200
                offset += len(data)
                data = f.read(10000)

        assert resp.headers["Tus-Upload-Finished"] == "1"

    processing = get_processing()
    transaction = get_transaction()

    sub = await transaction.js.pull_subscribe("nucliadb.1", "auto")
    msgs = await sub.fetch(2)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[1].data)
    await msgs[1].ack()

    payload = processing.calls[1]

    assert payload["kbid"] == kb
    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
    assert payload["filefield"][field] == "DUMMYJWT"
    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpg"
    assert writer.basic.title == ""
    assert writer.files[field].language == "ca"
    assert writer.files[field].file.size == 30472
    assert writer.files[field].file.filename == "image.jpg"
    assert writer.files[field].file.md5 == "7af0916dba8b70e29d99e72941923529"

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472


@pytest.mark.asyncio
async def test_file_upload_by_slug(writer_api, knowledgebox_writer):
    kb = knowledgebox_writer
    rslug = "myslug"

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kb}/resources",
            headers={
                "X-Synchronous": "True",
            },
            json={
                "slug": rslug,
            },
        )
        assert str(resp.status_code).startswith("2")

        filename = "image.jpg"
        with open(f"{ASSETS_PATH}/image001.jpg", "rb") as f:
            resp = await client.post(
                f"/{KB_PREFIX}/{kb}/{RSLUG_PREFIX}/{rslug}/file/file1/{UPLOAD}",
                data=f.read(),
                headers={
                    "X-FILENAME": filename,
                    "content-type": "image/jpg",
                    "X-MD5": "7af0916dba8b70e29d99e72941923529",
                },
            )
            assert resp.status_code == 201

    processing = get_processing()
    transaction = get_transaction()

    sub = await transaction.js.pull_subscribe("nucliadb.1", "auto")
    msgs = await sub.fetch(2)

    writer = BrokerMessage()
    writer.ParseFromString(msgs[-1].data)
    await msgs[-1].ack()

    payload = processing.calls[-1]

    assert payload["kbid"] == kb
    body = resp.json()
    field = body["field_id"]
    rid = body["uuid"]
    assert payload["filefield"][field] == "DUMMYJWT"

    assert writer.uuid == rid
    assert writer.basic.icon == "image/jpg"
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
