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
import base64
import datetime
import uuid
from contextlib import AsyncExitStack
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import aiohttp
import jwt  # type: ignore
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.resources_pb2 import FieldFile as FieldFilePB
from pydantic import BaseModel, Field

import nucliadb.models as models
from nucliadb_utils import logger
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError
from nucliadb_utils.storages.storage import Storage

if TYPE_CHECKING:
    SourceValue = CloudFile.Source.V
else:
    SourceValue = int


class Source(SourceValue, Enum):  # type: ignore
    HTTP = 0
    INGEST = 1


class PushPayload(BaseModel):
    # There are multiple options of payload
    uuid: str
    slug: Optional[str] = None
    kbid: str
    source: Optional[Source] = None
    userid: str

    genericfield: Dict[str, models.Text] = {}

    # New File
    filefield: Dict[str, str] = {}

    # New Link
    linkfield: Dict[str, models.LinkUpload] = {}

    # Diff on Text Field
    textfield: Dict[str, models.Text] = {}

    # Diff on a Layout Field
    layoutfield: Dict[str, models.LayoutDiff] = {}

    # New conversations to process
    conversationfield: Dict[str, models.PushConversation] = {}

    # Only internal
    partition: int

    # List of available processing options (with default values)
    processing_options: Optional[models.PushProcessingOptions] = Field(
        default_factory=models.PushProcessingOptions
    )


class PushResponse(BaseModel):
    seqid: Optional[int] = None


class ProcessingEngine:
    def __init__(
        self,
        nuclia_service_account: Optional[str] = None,
        nuclia_zone: Optional[str] = None,
        nuclia_public_url: Optional[str] = None,
        nuclia_cluster_url: Optional[str] = None,
        onprem: Optional[bool] = False,
        nuclia_jwt_key: Optional[str] = None,
        days_to_keep: int = 3,
        driver: str = "gcs",
        dummy: bool = False,
        disable_send_to_process: bool = False,
    ):
        self.disable_send_to_process = disable_send_to_process
        self.nuclia_service_account = nuclia_service_account
        self.nuclia_zone = nuclia_zone
        if nuclia_public_url is not None:
            self.nuclia_public_url: Optional[str] = nuclia_public_url.format(
                zone=nuclia_zone
            )
        else:
            self.nuclia_public_url = None

        if nuclia_cluster_url is not None:
            self.nuclia_cluster_url: Optional[str] = nuclia_cluster_url
        else:
            self.nuclia_cluster_url = None

        self.onprem = onprem
        if self.onprem:
            self.nuclia_upload_url = (
                f"{self.nuclia_public_url}/api/v1/processing/upload"
            )
        else:
            self.nuclia_upload_url = (
                f"{self.nuclia_cluster_url}/api/v1/processing/upload"
            )
        self.nuclia_internal_push = (
            f"{self.nuclia_cluster_url}/api/internal/processing/push"
        )
        self.nuclia_external_push = f"{self.nuclia_public_url}/api/v1/processing/push"

        self.nuclia_jwt_key = nuclia_jwt_key
        self.days_to_keep = days_to_keep
        if driver == "gcs":
            self.driver = 0
        elif driver == "s3":
            self.driver = 1
        else:
            logger.error(f"Not valid driver to processing: {driver}")
            self.driver = 2
        self._exit_stack = AsyncExitStack()

        # For dummy utility
        self.dummy = dummy
        self.calls: List[Dict[str, Any]] = []
        self.uploads: List[Dict[str, str]] = []

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()

    def generate_file_token_from_cloudfile(self, cf: CloudFile) -> str:
        if self.nuclia_jwt_key is None:
            raise AttributeError("Nuclia JWT key not set")
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        expiration = now + datetime.timedelta(days=self.days_to_keep)

        payload = {
            "iss": "urn:nucliadb",
            "sub": "file",
            "aud": "urn:proxy",
            "exp": expiration,
            "iat": now,
            "md5": cf.md5,
            "source": 1,  # To indicate that this files comes internally
            "driver": self.driver,
            "jti": uuid.uuid4().hex,
            "bucket_name": cf.bucket_name,
            "filename": cf.filename,
            "uri": cf.uri,
            "size": cf.size,
            "content_type": cf.content_type,
        }
        return jwt.encode(payload, self.nuclia_jwt_key, algorithm="HS256")

    def generate_file_token_from_fieldfile(self, file: FieldFilePB) -> str:
        if self.nuclia_jwt_key is None:
            raise AttributeError("Nuclia JWT key not set")
        now = datetime.datetime.now(tz=datetime.timezone.utc)
        expiration = now + datetime.timedelta(days=self.days_to_keep)

        payload = {
            "iss": "urn:nucliadb",
            "sub": "file",
            "aud": "urn:proxy",
            "exp": expiration,
            "iat": now,
            "md5": file.file.md5,
            "source": 1,  # To indicate that this files comes internally
            "driver": self.driver,
            "jti": uuid.uuid4().hex,
            "bucket_name": file.file.bucket_name,
            "filename": file.file.filename,
            "uri": file.file.uri,
            "size": file.file.size,
            "content_type": file.file.content_type,
            "password": file.password,
            "language": file.language,
        }
        return jwt.encode(payload, self.nuclia_jwt_key, algorithm="HS256")

    async def convert_filefield_to_str(self, file: models.FileField) -> str:
        # Upload file without storing on Nuclia DB
        headers = {}
        headers["X-PASSWORD"] = file.password
        headers["X-LANGUAGE"] = file.language
        headers["X-FILENAME"] = base64.b64encode(file.file.filename.encode()).decode()  # type: ignore
        headers["X-MD5"] = file.file.md5
        headers["CONTENT_TYPE"] = file.file.content_type
        headers["CONTENT-LENGTH"] = str(len(file.file.payload))  # type: ignore
        headers["X-STF-NUAKEY"] = f"Bearer {self.nuclia_service_account}"
        with self.session.post(
            self.nuclia_upload_url, data=file.file.payload, headers=headers
        ) as resp:
            assert resp.status == 200
            jwttoken = await resp.text()
        return jwttoken

    def convert_external_filefield_to_str(self, file_field: models.FileField) -> str:
        if self.nuclia_jwt_key is None:
            raise AttributeError("Nuclia JWT key not set")

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        expiration = now + datetime.timedelta(days=self.days_to_keep)
        payload = {
            "iss": "urn:nucliadb",
            "sub": "file",
            "aud": "urn:proxy",
            "iat": now,
            "exp": expiration,
            "jti": uuid.uuid4().hex,
            "source": 1,  # To indicate that this files comes internally
            "driver": 3,  # To indicate that this is an externally-hosted file (no gcp, s3 nor local).
            "uri": file_field.file.uri,
            "extra_headers": file_field.file.extra_headers,
        }
        return jwt.encode(payload, self.nuclia_jwt_key, algorithm="HS256")

    async def convert_internal_filefield_to_str(
        self, file: FieldFilePB, storage: Storage
    ) -> str:
        """It's already an internal file that needs to be uploaded"""
        if self.onprem is False:
            # Upload the file to processing upload
            jwttoken = self.generate_file_token_from_fieldfile(file)
        elif self.disable_send_to_process:
            return ""
        else:
            headers = {}
            headers["X-PASSWORD"] = file.password
            headers["X-LANGUAGE"] = file.language
            headers["X-FILENAME"] = base64.b64encode(
                file.file.filename.encode()
            ).decode()
            headers["X-MD5"] = file.file.md5
            headers["CONTENT-TYPE"] = file.file.content_type
            if file.file.size:
                headers["CONTENT-LENGTH"] = str(file.file.size)
            headers["X-STF-NUAKEY"] = f"Bearer {self.nuclia_service_account}"
            if self.dummy:
                self.uploads.append(headers)
                return "DUMMYJWT"

            iterator = storage.downloadbytescf_iterator(file.file)
            async with self.session.post(
                self.nuclia_upload_url, data=iterator, headers=headers
            ) as resp:
                if resp.status == 200:
                    jwttoken = await resp.text()
                else:
                    text = await resp.text()
                    raise Exception(f"STATUS: {resp.status} - {text}")
        return jwttoken

    async def convert_internal_cf_to_str(self, cf: CloudFile, storage: Storage) -> str:
        if self.onprem is False:
            # Upload the file to processing upload
            jwttoken = self.generate_file_token_from_cloudfile(cf)
        elif self.disable_send_to_process:
            return ""
        else:
            headers = {}
            headers["X-FILENAME"] = base64.b64encode(cf.filename.encode()).decode()
            headers["X-MD5"] = cf.md5
            headers["CONTENT-TYPE"] = cf.content_type
            if cf.size:
                headers["CONTENT-LENGTH"] = str(cf.size)
            headers["X-STF-NUAKEY"] = f"Bearer {self.nuclia_service_account}"
            if self.dummy:
                self.uploads.append(headers)
                return "DUMMYJWT"

            iterator = storage.downloadbytescf_iterator(cf)
            with self.session.post(
                self.nuclia_upload_url, data=iterator, headers=headers
            ) as resp:
                if resp.status == 200:
                    jwttoken = await resp.text()
                else:
                    text = await resp.text()
                    raise Exception(f"STATUS: {resp.status} - {text}")

        return jwttoken

    async def send_to_process(self, item: PushPayload, partition: int) -> int:
        if self.disable_send_to_process:
            return 0

        if self.dummy:
            self.calls.append(item.dict())
            return len(self.calls)

        headers = {"CONTENT-TYPE": "application/json"}
        if self.onprem is False:
            # Upload the payload
            item.partition = partition
            resp = await self.session.post(
                url=f"{self.nuclia_internal_push}", data=item.json(), headers=headers
            )
            if resp.status == 200:
                data = await resp.json()
                seqid = data.get("seqid")
            elif resp.status == 402:
                data = await resp.json()
                raise LimitsExceededError(data["detail"])
            else:
                raise SendToProcessError(f"{resp.status}: {await resp.text()}")
        else:

            headers.update({"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"})
            # Upload the payload
            resp = await self.session.post(
                url=self.nuclia_external_push + "?partition=" + str(partition),
                data=item.json(),
                headers=headers,
            )
            if resp.status == 200:
                data = await resp.json()
                seqid = data.get("seqid")
            elif resp.status == 402:
                raise LimitsExceededError(data["detail"])
            else:
                raise SendToProcessError(f"{resp.status}: {await resp.text()}")
        logger.info(
            f"Pushed message to proxy. kb: {item.kbid}, resource: {item.uuid}, \
                ingest seqid: {seqid}, partition: {partition}"
        )
        return seqid
