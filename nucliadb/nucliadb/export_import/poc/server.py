import storage
from fastapi import FastAPI
from shared import SERVER_HOST, SERVER_PORT, encode_resource, encode_binary
from starlette.requests import Request
from starlette.responses import StreamingResponse


api = FastAPI()

EOL = b"\n"


def iter_file_chunks(path):
    with open(path, mode="rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            yield chunk



def export_to_stream(kbid: str):
    bins = storage.get_binaries(kbid)

    for resource in storage.get_resources(kbid):
        enc_res = encode_resource(resource)
        enc_res_size = len(enc_res).to_bytes(4, byteorder="big")

        yield enc_res_size + EOL
        yield enc_res + EOL

        rbin = bins.get(resource.id, None)
        if rbin is not None:
            enc_bin = encode_binary(rbin)
            enc_bin_size = len(enc_bin).to_bytes(4, byteorder="big")

            yield enc_bin_size + EOL
            yield enc_bin + EOL


async def stream_lines(request: Request):
    async for chunk in request.stream():
        lines = chunk.split(EOL)
        for line in lines:
            yield line


async def import_from_stream(request: Request, kbid: str):
    gen = stream_lines(request)
    while True:
        try:
            line = await gen.__anext__()
            if not line:
                continue
            print(line)
        except StopAsyncIteration:
            break


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
