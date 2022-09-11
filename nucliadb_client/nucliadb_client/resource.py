from typing import Iterable, Optional, TYPE_CHECKING
import httpx

from nucliadb_models.resource import Resource as NucliaDBResource
from nucliadb_protos.writer_pb2 import BrokerMessage

if TYPE_CHECKING:
    from nucliadb_client.knowledgebox import KnowledgeBox

RESOURCE_PREFIX = "resource"


class Resource:
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

    def apply(self, broker_messages: Iterable[BrokerMessage]):
        self.kb.client.writer_stub.ProcessMessage(broker_messages)
