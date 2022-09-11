from typing import List, Optional, TYPE_CHECKING

from nucliadb_models.resource import KnowledgeBoxObj, ResourceList
from nucliadb_models.writer import CreateResourcePayload, ResourceCreated

if TYPE_CHECKING:
    from nucliadb_client.client import NucliaDBClient
import httpx

from nucliadb_client.resource import Resource

KB_PREFIX = "kb"


class KnowledgeBox:
    def __init__(self, kbid: str, client: "NucliaDBClient", slug: Optional[str] = None):
        self.kbid = kbid
        self.client = client
        self.http_reader_v1 = httpx.Client(
            base_url=f"{client.http_reader_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "READER"},
            follow_redirects=True,
        )
        self.http_writer_v1 = httpx.Client(
            base_url=f"{client.http_reader_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "WRITER"},
            follow_redirects=True,
        )
        self.http_manager_v1 = httpx.Client(
            base_url=f"{client.http_reader_v1.base_url}{KB_PREFIX}/{kbid}",
            headers={"X-NUCLIADB-ROLES": "MANAGER"},
            follow_redirects=True,
        )
        self.slug = slug

    def get(self) -> KnowledgeBoxObj:
        response = self.http_manager_v1.get("").content
        return KnowledgeBoxObj.parse_raw(response)

    def list_elements(self, page: int = 0, size: int = 20) -> List[Resource]:
        response = self.http_manager_v1.get(f"resources?page={page}&size={size}")
        response_obj = ResourceList.parse_raw(response.content)
        result = []
        for resource in response_obj.resources:
            result.append(Resource(rid=resource.id, kb=self))
        return result

    def create_resource(self, payload: CreateResourcePayload) -> Resource:
        response = self.http_writer_v1.post(f"resources", data=payload.json())
        response_obj = ResourceCreated.parse_raw(response.content)
        return Resource(rid=response_obj.uuid, kb=self)

    def delete(self):
        resp = self.http_manager_v1.delete("")
        return resp.status_code == 200
