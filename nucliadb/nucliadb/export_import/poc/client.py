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


def export_kb(client, kbid: str, export_path=None) -> str:
    resp = client.get(f"/export/{kbid}")
    assert resp.status_code == 200
    export_path = export_path or f"{kbid}.export"
    with open(export_path, mode="wb") as file:
        for chunk in resp.iter_bytes():
            file.write(chunk)
    return export_path


def import_kb(client, kbid: str, export: str) -> None:
    chunk_size = 1024 * 1024
    def stream_export():
        with open(export, mode="r") as file:
            for chunk in file.read(chunk_size):
                yield chunk.encode("utf-8")

    resp = client.post(f"/import/{kbid}", content=stream_export())
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
    path = Path("kb1.export")
    export = export_kb(client, "kb1", path)
    import_kb(client, "kb2", export)
    assert list_resources(client, "kb2") == list_resources(client, "kb1")
    assert list_binaries(client, "kb2") == list_binaries(client, "kb1")
