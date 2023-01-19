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

import logging
from base64 import b64encode
from hashlib import md5
from typing import TYPE_CHECKING, Dict, List, Optional

import httpx
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldType,
    Metadata,
    Paragraph,
)
from nucliadb_protos.train_pb2 import GetSentencesRequest
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_models.file import FileField
from nucliadb_models.resource import Resource as NucliaDBResource

if TYPE_CHECKING:
    from nucliadb_client.knowledgebox import KnowledgeBox


logger = logging.getLogger("nucliadb_client")

RESOURCE_PREFIX = "resource"
TUS_CHUNK_SIZE = 524288


class Resource:
    _bm: Optional[BrokerMessage] = None
    http_reader_v1: httpx.Client
    http_writer_v1: httpx.Client
    http_manager_v1: httpx.Client

    def __init__(
        self, rid: Optional[str], kb: "KnowledgeBox", slug: Optional[str] = None
    ):
        self.rid = rid
        self.kb = kb
        self.http_reader_v1 = httpx.Client(
            base_url=f"{kb.http_reader_v1.base_url}{RESOURCE_PREFIX}/{rid}",
            headers={"X-NUCLIADB-ROLES": "READER"},
            follow_redirects=True,
        )
        self.http_writer_v1: httpx.Client = httpx.Client(
            base_url=f"{kb.http_writer_v1.base_url}{RESOURCE_PREFIX}/{rid}",
            headers={"X-NUCLIADB-ROLES": "WRITER"},
            follow_redirects=True,
        )
        self.http_manager_v1: httpx.Client = httpx.Client(
            base_url=f"{kb.http_manager_v1.base_url}{RESOURCE_PREFIX}/{rid}",
            headers={"X-NUCLIADB-ROLES": "MANAGER"},
            follow_redirects=True,
        )
        self.slug = slug

    def get(
        self,
        show: Optional[List[str]] = None,
        field_type: Optional[List[str]] = None,
        extracted: Optional[List[str]] = None,
        timeout: Optional[int] = None,
    ) -> NucliaDBResource:
        params = {}

        show = show or []
        if show:
            params["show"] = show

        field_type = field_type or []
        if field_type:
            params["field_type"] = field_type

        if extracted:
            if "extracted" not in show:
                show.append("extracted")
            params["extracted"] = extracted

        response = self.http_reader_v1.get("", params=params, timeout=timeout).content
        return NucliaDBResource.parse_raw(response)

    def update(self):
        response = self.http_reader_v1.get("").content
        return NucliaDBResource.parse_raw(response)

    def delete(self):
        response = self.http_writer_v1.delete("")
        return NucliaDBResource.parse_raw(response)

    def reprocess(self):
        response = self.http_writer_v1.post("reprocess")
        assert response.status_code == 202

    def reindex(self, vectors: bool = False, timeout=None):
        response = self.http_writer_v1.post(
            "reindex", params={"reindex_vectors": vectors}, timeout=timeout
        )
        assert response.status_code == 200

    def download_file(self, field_id):
        resp = self.http_reader_v1.get(f"/file/{field_id}/download/field")
        assert resp.status_code == 200
        return resp

    @property
    def bm(self) -> BrokerMessage:
        if self._bm is None:
            self._bm = BrokerMessage()
        return self._bm

    def upload_file(self, file_id: str, field: FileField, wait: bool = False):
        if field.file.payload is None:
            raise ValueError("Need a field binary to upload")
        headers: Dict[str, str] = {
            "X-FILENAME": field.file.filename or "",
            "X-MD5": md5(field.file.payload.encode()).hexdigest(),
            "Content-Type": field.file.content_type or "application/octet-strem",
            "Content-Length": str(len(field.file.payload)),
        }
        if wait:
            headers["X-SYNCHRONOUS"] = "True"
        resp = self.http_writer_v1.post(
            f"/file/{file_id}/upload",
            content=field.file.payload,
            headers=headers,
        )
        assert resp.status_code == 201

    def _tus_post(self, file_id: str, field: FileField) -> str:
        upload_metadata = []
        md5_hash = md5(field.file.payload.encode()).hexdigest()  # type: ignore
        b64_md5_hash = b64encode(md5_hash.encode()).decode()
        upload_metadata.append(f"md5 {b64_md5_hash}")
        if field.file.filename:
            b64_filename = b64encode(field.file.filename.encode()).decode()
            upload_metadata.append(f"filename {b64_filename}")
        if field.file.content_type:
            b64_content_type = b64encode(field.file.content_type.encode()).decode()
            upload_metadata.append(f"content_type {b64_content_type}")
        headers = {
            "upload-length": str(len(field.file.payload)),  # type: ignore
            "tus-resumable": "1.0.0",
            "upload-metadata": ",".join(upload_metadata),
        }
        resp = self.http_writer_v1.post(
            f"/file/{file_id}/tusupload",
            headers=headers,
        )
        assert resp.status_code == 201
        return resp.headers["location"]

    def _tus_patch(self, upload_id: str, field: FileField, wait: bool = False) -> None:
        headers = {}
        if wait:
            headers["X-SYNCHRONOUS"] = "True"
        pos = 0
        while True:
            headers["upload-offset"] = str(pos)
            chunk = field.file.payload[pos:TUS_CHUNK_SIZE]  # type: ignore
            resp = self.kb.http_writer_v1.patch(
                f"/tusupload/{upload_id}", headers=headers, data=chunk  # type: ignore
            )
            assert resp.status_code == 200
            if len(chunk) < TUS_CHUNK_SIZE:
                # Finished
                break
            pos += len(chunk)

    def tus_upload_file(self, file_id: str, field: FileField, wait: bool = False):
        if field.file.payload is None:
            raise ValueError("Need a field binary to upload")
        location = self._tus_post(file_id, field)
        upload_id = location.split("/")[-1]
        self._tus_patch(upload_id, field, wait=wait)

    def add_vectors(
        self,
        field: str,
        field_type: FieldType,
        vectors: List[Vector],
        split: Optional[str] = None,
    ):

        evw = ExtractedVectorsWrapper()
        evw.field.field = field
        evw.field.field_type = field_type  # type: ignore
        for vector in vectors:
            if split is not None:
                evw.vectors.split_vectors[split].vectors.append(vector)
            else:
                evw.vectors.vectors.vectors.append(vector)

        self.bm.field_vectors.append(evw)

    def add_text(
        self,
        field: str,
        field_type: FieldType,
        text: str,
        split: Optional[str] = None,
    ):

        etw = ExtractedTextWrapper()
        etw.field.field = field
        etw.field.field_type = field_type  # type: ignore
        if split is not None:
            etw.body.split_text[split] = text
        else:
            etw.body.text = text

        self.bm.extracted_text.append(etw)

        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.field = field
        fcmw.field.field_type = field_type  # type: ignore
        if split is not None:
            fcmw.metadata.split_metadata[split].paragraphs.append(
                Paragraph(
                    start=0,
                    end=len(text),
                    kind=Paragraph.TypeParagraph.TEXT,
                )
            )
        else:
            fcmw.metadata.metadata.paragraphs.append(
                Paragraph(
                    start=0,
                    end=len(text),
                    kind=Paragraph.TypeParagraph.TEXT,
                )
            )
        self.bm.field_metadata.append(fcmw)

    def sync_commit(self):
        if self.bm is None:
            raise AttributeError("No Broker Message")

        if self.rid is None:
            raise AttributeError("Not initialized")

        self.bm.uuid = self.rid
        self.bm.kbid = self.kb.kbid
        self.bm.type = BrokerMessage.MessageType.AUTOCOMMIT
        self.bm.source = BrokerMessage.MessageSource.PROCESSOR
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.status = Metadata.Status.PROCESSED

        def iterator():
            yield self.bm

        self.kb.client.writer_stub.ProcessMessage(iterator())

        self.cleanup()

    def parse(self, payload: bytes):
        self._bm = BrokerMessage()
        self._bm.ParseFromString(payload)
        self.rid = self.bm.uuid
        self.slug = self.bm.slug

    def serialize(self, processor: bool = True) -> bytes:
        if self.bm is None:
            raise AttributeError("No Broker Message")

        if self.rid is None:
            raise AttributeError("Not initialized")

        self.bm.uuid = self.rid
        self.bm.kbid = self.kb.kbid
        self.bm.type = BrokerMessage.MessageType.AUTOCOMMIT
        if processor:
            self.bm.source = BrokerMessage.MessageSource.PROCESSOR
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.status = Metadata.Status.PROCESSED

        return self.bm.SerializeToString()

    def cleanup(self):
        self._bm = None

    def iter_sentences(self):
        if not self.kb.client.train_stub:
            raise RuntimeError("Train stub not configured")

        request = GetSentencesRequest()
        request.kb.uuid = self.kb.kbid
        request.uuid = self.rid
        request.metadata.text = True

        for sentence in self.kb.client.train_stub.GetSentences(request):
            yield sentence

    async def commit(self, processor: bool = True):
        if self.bm is None:
            raise AttributeError("No Broker Message")

        if self.kb.client.writer_stub_async is None:
            raise AttributeError("Writer Stub Async not initialized")

        if self.rid is None:
            raise AttributeError("Not initialized")

        self.bm.uuid = self.rid
        self.bm.kbid = self.kb.kbid
        self.bm.type = BrokerMessage.MessageType.AUTOCOMMIT
        if processor:
            self.bm.source = BrokerMessage.MessageSource.PROCESSOR
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.status = Metadata.Status.PROCESSED

        def iterator():
            yield self.bm

        await self.kb.client.writer_stub_async.ProcessMessage(iterator())  # type: ignore
        logger.info(f"Commited {self.bm.uuid}")

        self.cleanup()
