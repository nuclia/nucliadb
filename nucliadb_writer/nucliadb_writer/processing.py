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
import datetime
import uuid
from contextlib import AsyncExitStack
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import aiohttp
import jwt
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.resources_pb2 import FieldFile as FieldFilePB
from pydantic import BaseModel

import nucliadb_models as models
from nucliadb_utils.storages.storage import Storage
from nucliadb_writer import logger
from nucliadb_writer.exceptions import LimitsExceededError, SendToProcessError

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
    txseqid: Optional[int] = None

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
    ):
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
            logger.error(f"Not valid driver to processing {driver}")
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
            raise AttributeError()
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
            raise AttributeError()
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
        headers["X-FILENAME"] = file.file.filename
        headers["X-MD5"] = file.file.md5
        headers["CONTENT_TYPE"] = file.file.content_type
        headers["AUTHORIZATION"] = f"Bearer {self.nuclia_service_account}"
        headers["X-STF_ROLES"] = "WRITER"
        with self.session.post(
            self.nuclia_upload_url, data=file.file.payload, headers=headers
        ) as resp:
            jwt = resp
        return jwt

    async def convert_internal_filefield_to_str(
        self, file: FieldFilePB, storage: Storage
    ) -> str:
        """ITs already an internal file that needs to be uploaded"""
        if self.onprem is False:
            # Upload the file to processing upload
            jwt = self.generate_file_token_from_fieldfile(file)
        else:
            headers = {}
            headers["X-PASSWORD"] = file.password
            headers["X-LANGUAGE"] = file.language
            headers["X-FILENAME"] = file.file.filename
            headers["X-MD5"] = file.file.md5
            headers["CONTENT_TYPE"] = file.file.content_type
            headers["AUTHORIZATION"] = f"Bearer {self.nuclia_service_account}"
            headers["X-STF_ROLES"] = "WRITER"
            if self.dummy:
                self.uploads.append(headers)
                return "DUMMYJWT"

            iterator = storage.downloadbytescf_iterator(file.file)
            async with self.session.post(
                self.nuclia_upload_url, data=iterator, headers=headers
            ) as resp:
                jwt = resp
        return jwt

    async def convert_internal_cf_to_str(self, cf: CloudFile, storage: Storage) -> str:
        if self.onprem is False:
            # Upload the file to processing upload
            jwt = self.generate_file_token_from_cloudfile(cf)
        else:
            headers = {}
            headers["X-FILENAME"] = cf.filename
            headers["X-MD5"] = cf.md5
            headers["CONTENT_TYPE"] = cf.content_type
            headers["AUTHORIZATION"] = f"Bearer {self.nuclia_service_account}"
            headers["X-STF_ROLES"] = "WRITER"
            if self.dummy:
                self.uploads.append(headers)
                return "DUMMYJWT"

            iterator = storage.downloadbytescf_iterator(cf)
            with self.session.post(
                self.nuclia_upload_url, data=iterator, headers=headers
            ) as resp:
                jwt = resp

        return jwt

    async def send_to_process(
        self, item: PushPayload, partition: int
    ) -> Tuple[int, str]:
        if self.dummy:
            self.calls.append(item.dict())
            return 1, "1"

        if self.onprem is False:
            # Upload the payload
            item.partition = partition
            resp = await self.session.post(
                url=f"{self.nuclia_internal_push}",
                json=item.dict(),
            )
            if resp.status == 200:
                data = await resp.json()
                processing_id = data.get("processing_id")
                seqid = data.get("seqid")

            if resp.status == 412:
                raise LimitsExceededError(data["detail"])
            else:
                raise SendToProcessError(f"{resp.status}: {resp.content}")
        else:
            headers = {"Authorization": f"Bearer {self.nuclia_service_account}"}
            # Upload the payload
            resp = await self.session.post(
                url=self.nuclia_external_push + "?partition=" + str(partition),
                json=item.dict(),
                headers=headers,
            )
            if resp.status == 200:
                data = await resp.json()
                processing_id = data.get("processing_id")
                seqid = data.get("seqid")
            else:
                raise SendToProcessError(f"{resp.status}: {resp.content}")
        logger.info(
            f"Pushed message to proxy. kb: {item.kbid}, resource: {item.uuid}, ingest seqid: {seqid}, partition: {partition}"
        )
        return seqid, processing_id
