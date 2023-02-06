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
from typing import TYPE_CHECKING, List, Optional

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

from nucliadb_models.resource import Resource as NucliaDBResource

if TYPE_CHECKING:
    from nucliadb_client.knowledgebox import KnowledgeBox

RESOURCE_PREFIX = "resource"
logger = logging.getLogger("nucliadb_client")


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
        return resp.content

    @property
    def bm(self) -> BrokerMessage:
        if self._bm is None:
            self._bm = BrokerMessage()
        return self._bm

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

        self.kb.client.writer_stub.ProcessMessage(self._iterator())
        logger.info(f"Commited {self.bm.uuid}")

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

    @property
    def writer_bm(self):
        wbm = BrokerMessage()
        wbm.CopyFrom(self.bm)
        # We clear the fields that are typically populated on the
        # processor's broker message.
        for computed_field in [
            "link_extracted_data",
            "file_extracted_data",
            "extracted_text",
            "field_metadata",
            "field_vectors",
            "field_large_metadata",
            "user_vectors",
        ]:
            wbm.ClearField(computed_field)
        wbm.type = BrokerMessage.MessageType.AUTOCOMMIT
        wbm.source = BrokerMessage.MessageSource.WRITER
        wbm.basic.metadata.useful = True
        wbm.basic.metadata.status = Metadata.Status.PENDING
        return wbm

    @property
    def processor_bm(self):
        pbm = BrokerMessage()
        pbm.CopyFrom(self.bm)
        # We clear the fields that are typically populated on the
        # writer's broker message.
        for writer_field in [
            "links",
            "files",
            "texts",
            "conversations",
            "layouts",
            "keywordsets",
            "datetimes",
        ]:
            pbm.ClearField(writer_field)
        pbm.type = BrokerMessage.MessageType.AUTOCOMMIT
        pbm.source = BrokerMessage.MessageSource.PROCESSOR
        pbm.basic.metadata.useful = True
        pbm.basic.metadata.status = Metadata.Status.PROCESSED
        return pbm

    def _iterator(self):
        # We simulate the regular ingestion process: first a message from the writer
        # with only the basic metadata. Then a message from processing containing all
        # computed and extracted metadata
        yield self.writer_bm
        yield self.processor_bm

    async def commit(self):
        if self.bm is None:
            raise AttributeError("No Broker Message")

        if self.kb.client.writer_stub_async is None:
            raise AttributeError("Writer Stub Async not initialized")

        if self.rid is None:
            raise AttributeError("Not initialized")

        self.bm.uuid = self.rid
        self.bm.kbid = self.kb.kbid
        self.bm.type = BrokerMessage.MessageType.AUTOCOMMIT

        await self.kb.client.writer_stub_async.ProcessMessage(self._iterator())
        logger.info(f"Commited {self.bm.uuid}")

        self.cleanup()
