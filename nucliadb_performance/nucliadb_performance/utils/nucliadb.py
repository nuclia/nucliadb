from dataclasses import dataclass

from nucliadb_sdk import NucliaDB
from nucliadb_sdk.v2.exceptions import NotFoundError

LOCAL_API = "http://localhost:8080/api"
CLUSTER_API = "http://{service}.nucliadb.svc.cluster.local:8080/api"
ROLES_HEADER = "READER;WRITER;MANAGER"


@dataclass
class NucliaDBClient:
    reader: NucliaDB
    writer: NucliaDB


def get_nucliadb_client(local: bool = True) -> NucliaDBClient:
    if local:
        return NucliaDBClient(
            reader=NucliaDB(url=LOCAL_API, headers={"X-Nucliadb-Roles": ROLES_HEADER}),
            writer=NucliaDB(url=LOCAL_API, headers={"X-Nucliadb-Roles": ROLES_HEADER}),
        )
    return NucliaDBClient(
        reader=NucliaDB(
            url=CLUSTER_API.format(service="reader"),
            headers={"X-Nucliadb-Roles": ROLES_HEADER},
        ),
        writer=NucliaDB(
            url=CLUSTER_API.format(service="writer"),
            headers={"X-Nucliadb-Roles": ROLES_HEADER},
        ),
    )


def get_kbid(ndb, slug_or_kbid) -> str:
    try:
        kbid = ndb.reader.get_knowledge_box_by_slug(slug=slug_or_kbid).uuid
    except NotFoundError:
        kbid = ndb.reader.get_knowledge_box(kbid=slug_or_kbid)
    return kbid
