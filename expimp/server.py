import storage
from fastapi import FastAPI
from server_utils import iter_request_binaries, iter_request_resources
from shared import SERVER_HOST, SERVER_PORT, encode_binary, encode_resource
from starlette.requests import Request
from starlette.responses import StreamingResponse

api = FastAPI()


@api.get("/export/{kbid}/resources")
async def export_resources(kbid: str):
    return StreamingResponse(
        stream_resources(kbid),
        status_code=200,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={kbid}.resources.export"
        },
    )


@api.get("/export/{kbid}/binaries")
async def export_resources(kbid: str):
    return StreamingResponse(
        stream_binaries(kbid),
        status_code=200,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={kbid}.binaries.export"},
    )


@api.post("/import/{kbid}/resources")
async def import_resources(request: Request, kbid: str):
    storage.clear_resources(kbid)
    async for resource in iter_request_resources(request):
        storage.store_resource(kbid, resource)


@api.post("/import/{kbid}/binaries")
async def import_binaries(request: Request, kbid: str):
    storage.clear_binaries(kbid)
    async for rid, data in iter_request_binaries(request):
        storage.store_binary(kbid, rid, data)


def stream_resources(kbid: str):
    for resource in storage.get_resources(kbid):
        yield encode_resource(resource)


def stream_binaries(kbid: str):
    for rid, binary in storage.get_binaries(kbid):
        yield encode_binary(rid, binary)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(api, host=SERVER_HOST, port=SERVER_PORT)


"""
TODO:
- [ ] put some limitations on export / import size (n of resources, n of binaries, paragraphs, etc.)
- [ ] have the server store in a temporary file the whole export, and then stream it to the client.
- [ ] have the erver store the whole import in a temporary file, and then send it to process.
"""