from typing import List, Optional, TYPE_CHECKING
import httpx

from nucliadb_models.resource import Resource as NucliaDBResource
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldType,
    Metadata,
    Paragraph,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage

if TYPE_CHECKING:
    from nucliadb_client.knowledgebox import KnowledgeBox

RESOURCE_PREFIX = "resource"


class Resource:
    _bm: Optional[BrokerMessage] = None

    def __init__(self, rid: str, kb: "KnowledgeBox", slug: Optional[str] = None):
        self.rid = rid
        self.kb = kb
        self.http_reader_v1 = httpx.Client(
            base_url=f"{kb.http_reader_v1.base_url}{RESOURCE_PREFIX}/{rid}",
            headers={"X-NUCLIADB-ROLES": "READER"},
            follow_redirects=True,
        )
        self.http_writer_v1 = httpx.Client(
            base_url=f"{kb.http_reader_v1.base_url}{RESOURCE_PREFIX}/{rid}",
            headers={"X-NUCLIADB-ROLES": "WRITER"},
            follow_redirects=True,
        )
        self.http_manager_v1 = httpx.Client(
            base_url=f"{kb.http_reader_v1.base_url}{RESOURCE_PREFIX}/{rid}",
            headers={"X-NUCLIADB-ROLES": "MANAGER"},
            follow_redirects=True,
        )
        self.slug = slug

    def get(self, values=True) -> NucliaDBResource:
        show = []
        if values:
            show.append("values")
        response = self.http_reader_v1.get("?").content
        return NucliaDBResource.parse_raw(response)

    def update(self):
        response = self.http_writer_v1.get("").content
        return NucliaDBResource.parse_raw(response)

    def delete(self):
        response = self.http_writer_v1.delete("")
        return NucliaDBResource.parse_raw(response)

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
        evw.field.field_type = field_type
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
        etw.field.field_type = field_type
        if split is not None:
            etw.body.split_text[split] = text
        else:
            etw.body.text = text

        self.bm.extracted_text.append(etw)

        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.field = field
        fcmw.field.field_type = field_type
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

    def commit(self):
        if self.bm is None:
            raise AttributeError("No Broker Message")

        self.bm.uuid = self.rid
        self.bm.kbid = self.kb.kbid
        self.bm.type = BrokerMessage.MessageType.AUTOCOMMIT
        self.bm.source = BrokerMessage.MessageSource.PROCESSOR
        self.bm.basic.metadata.useful = True
        self.bm.basic.metadata.status = Metadata.Status.PROCESSED

        def iterator():
            yield self.bm

        self.kb.client.writer_stub.ProcessMessage(iterator())

        self._bm = None
