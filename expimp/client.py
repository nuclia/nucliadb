from pathlib import Path

import httpx
from pydantic import BaseModel
from shared import (
    SERVER_HOST,
    SERVER_PORT,
    Resource,
    decode_binary,
    decode_resource,
    encode_binary,
    encode_resource,
)


class KnowledgeBoxExport(BaseModel):
    kbid: str
    resources: list[Resource] = []
    binaries: dict[str, bytes] = {}


def get_resource(id: str) -> Resource:
    data_size = 1
    return Resource(id=id, data="D" * data_size)


def get_binary(size=5) -> bytes:
    return b"B" * size


def export_kb(client, kbid: str, binaries=True) -> KnowledgeBoxExport:
    export = KnowledgeBoxExport(kbid=kbid)
    export.resources = export_resources(client, kbid)
    if binaries:
        export.binaries = export_binaries(client, kbid)
    return export


def import_kb(client, kbid: str, export: KnowledgeBoxExport, binaries=True) -> None:
    if binaries:
        import_binaries(client, kbid, export.binaries)
    import_resources(client, kbid, export.resources)


def export_resources(client, kbid: str) -> list[Resource]:
    resp = client.get(f"/export/{kbid}/resources")
    assert resp.status_code == 200
    result = []
    for line in resp.iter_lines():
        resource = decode_resource(line)
        result.append(resource)
    return result


def import_resources(client, kbid: str, data: list[Resource]) -> None:
    def upload_generator():
        for resource in data:
            yield encode_resource(resource)

    resp = client.post(f"/import/{kbid}/resources", content=upload_generator())
    assert resp.status_code == 200


def export_binaries(client, kbid: str) -> list[tuple[str, bytes]]:
    resp = client.get(f"/export/{kbid}/binaries")
    assert resp.status_code == 200
    result = {}
    for line in resp.iter_lines():
        rid, data = decode_binary(line)
        result[rid] = data
    return result


def import_binaries(client, kbid: str, binaries: dict[str, bytes]) -> None:
    def upload_generator():
        for rid, data in binaries.items():
            yield encode_binary(rid, data)

    resp = client.post(f"/import/{kbid}/binaries", content=upload_generator())
    assert resp.status_code == 200


def export_tarfile(client, kbid: str, path: Path):
    resp = client.get(f"/export/{kbid}")
    assert resp.status_code == 200
    with path.open("wb") as f:
        for chunk in resp.iter_bytes():
            f.write(chunk)


def import_tarfile(client, kbid: str, path: Path) -> None:
    def upload_generator():
        with path.open("rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk

    resp = client.post(f"/import/{kbid}", content=upload_generator())
    assert resp.status_code == 200


def list_resources(client, kbid) -> list[str]:
    resp = client.get(f"/{kbid}/resources")
    assert resp.status_code == 200
    return resp.json()


def list_binaries(client, kbid) -> list[str]:
    resp = client.get(f"/{kbid}/binaries")
    assert resp.status_code == 200
    return resp.json()


if __name__ == "__main__":
    client = httpx.Client(base_url=f"http://{SERVER_HOST}:{SERVER_PORT}", timeout=None)
    path = Path("kb1.tar.gz")
    export_tarfile(client, "kb1", path)
    import_tarfile(client, "kb2", path)
    assert list_resources(client, "kb2") == list_resources(client, "kb1")
    assert list_binaries(client, "kb2") == list_binaries(client, "kb1")
