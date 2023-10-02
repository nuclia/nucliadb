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
from collections import defaultdict
from contextlib import AsyncExitStack
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypeVar

import aiohttp
import backoff
import jwt
from async_lru import alru_cache
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxID  # type: ignore
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.resources_pb2 import FieldFile as FieldFilePB
from nucliadb_protos.writer_pb2 import GetConfigurationResponse, OpStatusWriter
from pydantic import BaseModel, Field

import nucliadb_models as models
from nucliadb_models.configuration import KBConfiguration
from nucliadb_models.resource import QueueType
from nucliadb_telemetry import metrics
from nucliadb_utils import logger
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError
from nucliadb_utils.settings import nuclia_settings, storage_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import Utility, get_ingest, set_utility

_T = TypeVar("_T")

if TYPE_CHECKING:  # pragma: no cover
    SourceValue = CloudFile.Source.V
else:
    SourceValue = int

RETRIABLE_EXCEPTIONS = (aiohttp.client_exceptions.ClientConnectorError,)
MAX_TRIES = 4


processing_observer = metrics.Observer(
    "processing_engine",
    labels={"type": ""},
    error_mappings={
        "over_limits": LimitsExceededError,
        "processing_api_error": SendToProcessError,
    },
)


class Source(SourceValue, Enum):  # type: ignore
    HTTP = 0
    INGEST = 1


class ProcessingInfo(BaseModel):
    seqid: int
    account_seq: Optional[int]
    queue: QueueType


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

    learning_config: Optional[KBConfiguration] = None


class PushResponse(BaseModel):
    seqid: Optional[int] = None


async def start_processing_engine():
    if nuclia_settings.dummy_processing:
        processing_engine = DummyProcessingEngine()
    else:
        processing_engine = ProcessingEngine(
            nuclia_service_account=nuclia_settings.nuclia_service_account,
            nuclia_zone=nuclia_settings.nuclia_zone,
            onprem=nuclia_settings.onprem,
            nuclia_jwt_key=nuclia_settings.nuclia_jwt_key,
            nuclia_cluster_url=nuclia_settings.nuclia_cluster_url,
            nuclia_public_url=nuclia_settings.nuclia_public_url,
            driver=storage_settings.file_backend,
            days_to_keep=storage_settings.upload_token_expiration,
        )
    await processing_engine.initialize()
    set_utility(Utility.PROCESSING, processing_engine)


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
        elif driver in ("local", "pg"):
            self.driver = 2
        else:
            logger.error(
                f"Not a valid driver to processing, fallback to local: {driver}"
            )
            self.driver = 2
        self._exit_stack = AsyncExitStack()

    async def initialize(self):
        self.session = aiohttp.ClientSession()

    async def finalize(self):
        await self.session.close()

    @alru_cache(maxsize=None)
    async def get_configuration(self, kbid: str) -> Optional[KBConfiguration]:
        if self.onprem is False:
            return None

        ingest = get_ingest()
        kb_obj = KnowledgeBoxID()
        kb_obj.uuid = kbid
        pb_response: GetConfigurationResponse = await ingest.GetConfiguration(kb_obj)  # type: ignore
        if pb_response.status.status != OpStatusWriter.Status.OK:
            return None
        return KBConfiguration.from_message(pb_response.config)

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

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=MAX_TRIES)
    @processing_observer.wrap({"type": "file_field_upload"})
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
        async with self.session.post(
            self.nuclia_upload_url, data=file.file.payload, headers=headers
        ) as resp:
            if resp.status == 200:
                jwttoken = await resp.text()
                return jwttoken
            elif resp.status == 402:
                data = await resp.json()
                raise LimitsExceededError(resp.status, data["detail"])
            elif resp.status == 429:
                raise LimitsExceededError(resp.status, "Rate limited")
            else:
                text = await resp.text()
                raise Exception(f"STATUS: {resp.status} - {text}")

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
            "filename": file_field.file.filename,
            "content_type": file_field.file.content_type,
            "language": file_field.language,
            "password": file_field.password,
        }
        return jwt.encode(payload, self.nuclia_jwt_key, algorithm="HS256")

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=MAX_TRIES)
    @processing_observer.wrap({"type": "file_field_upload_internal"})
    async def convert_internal_filefield_to_str(
        self, file: FieldFilePB, storage: Storage
    ) -> str:
        """It's already an internal file that needs to be uploaded"""
        if self.onprem is False:
            # Upload the file to processing upload
            jwttoken = self.generate_file_token_from_fieldfile(file)
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

            iterator = storage.downloadbytescf_iterator(file.file)
            async with self.session.post(
                self.nuclia_upload_url, data=iterator, headers=headers
            ) as resp:
                if resp.status == 200:
                    jwttoken = await resp.text()
                elif resp.status == 402:
                    data = await resp.json()
                    raise LimitsExceededError(resp.status, data["detail"])
                elif resp.status == 429:
                    raise LimitsExceededError(resp.status, "Rate limited")
                else:
                    text = await resp.text()
                    raise Exception(f"STATUS: {resp.status} - {text}")
        return jwttoken

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=MAX_TRIES)
    @processing_observer.wrap({"type": "cloud_file_upload"})
    async def convert_internal_cf_to_str(self, cf: CloudFile, storage: Storage) -> str:
        if self.onprem is False:
            # Upload the file to processing upload
            jwttoken = self.generate_file_token_from_cloudfile(cf)
        else:
            headers = {}
            headers["X-FILENAME"] = base64.b64encode(cf.filename.encode()).decode()
            headers["X-MD5"] = cf.md5
            headers["CONTENT-TYPE"] = cf.content_type
            if cf.size:
                headers["CONTENT-LENGTH"] = str(cf.size)
            headers["X-STF-NUAKEY"] = f"Bearer {self.nuclia_service_account}"

            iterator = storage.downloadbytescf_iterator(cf)
            async with self.session.post(
                self.nuclia_upload_url, data=iterator, headers=headers
            ) as resp:
                if resp.status == 200:
                    jwttoken = await resp.text()
                elif resp.status == 402:
                    data = await resp.json()
                    raise LimitsExceededError(resp.status, data["detail"])
                elif resp.status == 429:
                    raise LimitsExceededError(resp.status, "Rate limited")
                else:
                    text = await resp.text()
                    raise Exception(f"STATUS: {resp.status} - {text}")

        return jwttoken

    @backoff.on_exception(backoff.expo, RETRIABLE_EXCEPTIONS, max_tries=MAX_TRIES)
    async def send_to_process(
        self, item: PushPayload, partition: int
    ) -> ProcessingInfo:
        op_type = "process_external" if self.onprem else "process_internal"
        with processing_observer({"type": op_type}):
            headers = {"CONTENT-TYPE": "application/json"}
            if self.onprem is False:
                # Upload the payload
                item.partition = partition
                resp = await self.session.post(
                    url=f"{self.nuclia_internal_push}",
                    data=item.json(),
                    headers=headers,
                )
            else:
                item.learning_config = await self.get_configuration(item.kbid)
                headers.update(
                    {"X-STF-NUAKEY": f"Bearer {self.nuclia_service_account}"}
                )
                # Upload the payload
                resp = await self.session.post(
                    url=self.nuclia_external_push + "?partition=" + str(partition),
                    data=item.json(),
                    headers=headers,
                )
            if resp.status == 200:
                data = await resp.json()
                seqid = data.get("seqid")
                account_seq = data.get("account_seq")
                queue_type = data.get("queue")
            elif resp.status in (402, 413):
                # 402 -> account limits exceeded
                # 413 -> payload size exceeded
                data = await resp.json()
                raise LimitsExceededError(resp.status, data["detail"])
            elif resp.status == 429:
                raise LimitsExceededError(resp.status, "Rate limited")
            else:
                error_text = await resp.text()
                logger.warning(f"Error sending to process: {resp.status} {error_text}")
                raise SendToProcessError()

        logger.info(
            f"Pushed message to proxy. kb: {item.kbid}, resource: {item.uuid}, \
                ingest seqid: {seqid}, partition: {partition}"
        )

        return ProcessingInfo(
            seqid=seqid, account_seq=account_seq, queue=QueueType(queue_type)
        )


class DummyProcessingEngine(ProcessingEngine):
    def __init__(self):
        self.calls: List[List[Any]] = []  # type: ignore
        self.values = defaultdict(list)
        self.onprem = True

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    async def convert_filefield_to_str(self, file: models.FileField) -> str:
        self.calls.append([file])
        index = len(self.values["convert_filefield_to_str"])
        self.values["convert_filefield_to_str"].append(file)
        return f"convert_filefield_to_str,{index}"

    def convert_external_filefield_to_str(self, file_field: models.FileField) -> str:
        self.calls.append([file_field])
        index = len(self.values["convert_external_filefield_to_str"])
        self.values["convert_external_filefield_to_str"].append(file_field)
        return f"convert_external_filefield_to_str,{index}"

    async def convert_internal_filefield_to_str(
        self, file: FieldFilePB, storage: Storage
    ) -> str:
        self.calls.append([file, storage])
        index = len(self.values["convert_internal_filefield_to_str"])
        self.values["convert_internal_filefield_to_str"].append([file, storage])
        return f"convert_internal_filefield_to_str,{index}"

    async def convert_internal_cf_to_str(self, cf: CloudFile, storage: Storage) -> str:
        self.calls.append([cf, storage])
        index = len(self.values["convert_internal_cf_to_str"])
        self.values["convert_internal_cf_to_str"].append([cf, storage])
        return f"convert_internal_cf_to_str,{index}"

    async def send_to_process(
        self, item: PushPayload, partition: int
    ) -> ProcessingInfo:
        self.calls.append([item, partition])
        item.learning_config = await self.get_configuration(item.kbid)

        self.values["send_to_process"].append([item, partition])
        return ProcessingInfo(
            seqid=len(self.calls), account_seq=0, queue=QueueType.SHARED
        )
