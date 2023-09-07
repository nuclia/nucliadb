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


if __name__ == "__main__":
    client = httpx.Client(base_url=f"http://{SERVER_HOST}:{SERVER_PORT}", timeout=None)

    crafted_export = KnowledgeBoxExport(kbid="kb1")
    crafted_export.resources = [get_resource("1"), get_resource("2")]
    crafted_export.binaries = {"1": get_binary(), "2": get_binary()}

    import_kb(client, "kb1", crafted_export)
    kb1_export = export_kb(client, "kb1")

    assert kb1_export == crafted_export
