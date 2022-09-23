import base64

import pytest

from nucliadb_models.resource import NucliaDBRoles
from nucliadb_writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb_writer.settings import settings as writer_settings
from nucliadb_writer.tests.test_files import ASSETS_PATH as WRITER_ASSETS_PATH
from nucliadb_writer.tus import TUSUPLOAD


@pytest.fixture(scope="function")
def configure_redis_dm(redis):
    writer_settings.dm_enabled = True
    writer_settings.dm_redis_host = redis[0]
    writer_settings.dm_redis_port = redis[1]
    yield


@pytest.mark.asyncio
async def test_file_tus_upload_and_download(
    nucliadb_api, configure_redis_dm, knowledgebox_one
):
    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        language = base64.b64encode(b"ca").decode()
        filename = "image.jpg"
        encoded_filename = base64.b64encode(filename.encode()).decode()
        md5 = base64.b64encode(b"7af0916dba8b70e29d99e72941923529").decode()

        kb_path = f"/{KB_PREFIX}/{knowledgebox_one}"
        resp = await client.post(
            f"{kb_path}/{RESOURCES_PREFIX}",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "slug": "resource1",
                "title": "Resource 1",
            },
        )
        assert resp.status_code == 201
        resource = resp.json().get("uuid")

        # Make the TUSUPLOAD post
        url = f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field1/{TUSUPLOAD}"
        resp = await client.post(
            url,
            headers={
                "tus-resumable": "1.0.0",
                "upload-metadata": f"filename {encoded_filename},language {language},md5 {md5}",
                "content-type": "image/jpg",
                "upload-defer-length": "1",
            },
        )
        assert resp.status_code == 201
        url = resp.headers["location"]

        offset = 0
        with open(f"{WRITER_ASSETS_PATH}/image001.jpg", "rb") as f:
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
                    "X-SYNCHRONOUS": "True",
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

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        download_url = (
            f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field1/download/field"
        )
        resp = await client.get(download_url)
        assert resp.status_code == 200
        assert (
            resp.headers["Content-Disposition"] == f'attachment; filename="{filename}"'
        )
