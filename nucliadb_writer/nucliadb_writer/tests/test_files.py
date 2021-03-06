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
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.utilities import get_storage, get_transaction
from nucliadb_writer.api.v1.router import KB_PREFIX
from nucliadb_writer.tus import TUSUPLOAD, UPLOAD
from nucliadb_writer.utilities import get_processing


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
        with open(os.path.dirname(__file__) + "/assets/image001.jpg", "rb") as f:
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

        with open(os.path.dirname(__file__) + "/assets/image001.jpg", "rb") as f:
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
    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
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

        with open(os.path.dirname(__file__) + "/assets/image001.jpg", "rb") as f:
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
        with open(os.path.dirname(__file__) + "/assets/image001.jpg", "rb") as f:
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
    path = resp.headers["ndb-field"]
    field = path.split("/")[-1]
    rid = path.split("/")[-3]
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
        with open(os.path.dirname(__file__) + "/assets/image001.jpg", "rb") as f:
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

        filename = base64.b64encode(b"image.jpg").decode()
        with open(os.path.dirname(__file__) + "/assets/image001.jpg", "rb") as f:
            resp = await client.post(
                f"/{KB_PREFIX}/{knowledgebox_writer}/resource/{resource}/file/field1/{UPLOAD}",
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

    storage = await get_storage()
    data = await storage.downloadbytes(
        bucket=writer.files[field].file.bucket_name,
        key=writer.files[field].file.uri,
    )
    assert len(data.read()) == 30472
