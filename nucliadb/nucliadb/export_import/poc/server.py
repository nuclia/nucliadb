import storage
from fastapi import FastAPI
from shared import (
    SERVER_HOST,
    SERVER_PORT,
    Codec,
    encode_binary,
    encode_resource,
    parse_codec,
)
from starlette.requests import Request
from starlette.responses import StreamingResponse

from nucliadb.export_import.poc.shared import decode_binary, decode_resource

api = FastAPI()


def iter_file_chunks(path):
    with open(path, mode="rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            yield chunk


class StreamReader:
    def __init__(self, stream):
        self.stream = stream
        self.gen = None
        self.buffer = b""

    def _read_from_buffer(self, n_bytes: int):
        value = self.buffer[:n_bytes]
        self.buffer = self.buffer[n_bytes:]
        return value

    async def read(self, n_bytes: int):
        if self.gen is None:
            self.gen = self.stream.__aiter__()

        while True:
            try:
                if self.buffer != b"" and len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)

                self.buffer += await self.gen.__anext__()
                if len(self.buffer) >= n_bytes:
                    return self._read_from_buffer(n_bytes)

            except StopAsyncIteration:
                if self.buffer != b"":
                    return self._read_from_buffer(n_bytes)
                else:
                    raise


class ImportStreamReader(StreamReader):
    async def iter_to_import(self):
        while True:
            try:
                size = await self.read(4)
                size_int = int.from_bytes(size, byteorder="big")
                encoded = await self.read(size_int)
                yield encoded
            except StopAsyncIteration:
                break


def import_resource(resource, kbid: str):
    storage.store_resource(kbid, resource)


def import_binary(binary, kbid: str, rid):
    storage.store_binary(kbid, rid, binary)


async def import_from_stream(request: Request, kbid: str):
    sr = ImportStreamReader(request.stream())
    async for encoded_item in sr.iter_to_import():
        codec, item = parse_codec(encoded_item)
        if codec == Codec.RESOURCE:
            resource = decode_resource(item)
            import_resource(resource, kbid)
        elif codec == Codec.BINARY:
            rid, binary = decode_binary(item)
            import_binary(binary, kbid, rid)
        else:
            continue


@api.get("/export/{kbid}")
async def export_endpoint(kbid: str):
    return StreamingResponse(
        export_to_stream(kbid),
        status_code=200,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={kbid}.resources.export"
        },
    )


@api.post("/import/{kbid}")
async def import_endpoint(request: Request, kbid: str):
    await import_from_stream(request, kbid)


@api.get("/{kbid}/binaries")
async def list_binaries(kbid: str):
    return {"binaries": len(storage.get_binaries(kbid))}


@api.get("/{kbid}/resources")
async def list_resources(kbid: str):
    return {"resources": [r.id for r in storage.get_resources(kbid)]}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(api, host=SERVER_HOST, port=SERVER_PORT)


"""
TODO:
- [ ] put some limitations on export / import size (n of resources, n of binaries, paragraphs, etc.)
- [x] have the server store in a temporary file the whole export, and then stream it to the client.
- [x] have the server store the whole import in a temporary file, and then send it to process.
"""
