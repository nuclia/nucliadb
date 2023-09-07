import tarfile
import tempfile
from pathlib import Path
from uuid import uuid4

import storage
from fastapi import FastAPI
from shared import SERVER_HOST, SERVER_PORT, decode_resource, encode_resource
from starlette.requests import Request
from starlette.responses import StreamingResponse

api = FastAPI()


def stream_from_tarfile(kbid: str):
    # Store everything in a temporary tar file and stream it to the client
    with tempfile.TemporaryDirectory() as tempfolder:
        path = generate_tarfile(kbid, tempfolder)
        for chunk in iter_file_chunks(path):
            yield chunk


def iter_file_chunks(path):
    with open(path, mode="rb") as file:
        while True:
            chunk = file.read(1024 * 1024)
            if not chunk:
                break
            yield chunk


async def stream_to_tarfile(request, kbid, tempfolder):
    # Store everything in a temporary tar file
    path = Path(f"{tempfolder}/{kbid}.tar.bz2")
    with open(path, "wb+") as file:
        async for chunk in request.stream():
            file.write(chunk)
    return path


def generate_tarfile(kbid: str, tempfolder: str):
    # Create a file where all encoded resources are stored
    resources_file_path = Path(f"{tempfolder}/resources")
    with open(resources_file_path, "w+") as resources_file:
        for resource in storage.get_resources(kbid):
            resources_file.write(encode_resource(resource).decode("utf-8"))

    # Create a tar file with all binaries and the resources file
    tarfile_path = Path(f"{tempfolder}/{kbid}.tar.bz2")
    with tarfile.open(f"{tempfolder}/{kbid}.tar.bz2", mode="w:bz2") as tar:
        tar.add(resources_file_path, "resources")
        for rid, bin in storage.get_binaries(kbid):
            filename = f"{tempfolder}/{uuid4().hex}"
            with open(filename, "wb+") as binary_file:
                binary_file.write(bin)
            tar.add(filename, rid)

    return tarfile_path


def import_tarfile(kbid, tempfolder, path):
    with tarfile.open(path, mode="r:bz2") as tar:
        extracted_path = Path(f"{tempfolder}/extracted")
        tar.extractall(path=extracted_path)
        with open(f"{extracted_path}/resources", "r") as resources_file:
            for line in resources_file.readlines():
                resource = decode_resource(line)
                storage.store_resource(kbid, resource)
        for member in tar.getmembers():
            if member.name != "resources":
                with open(f"{extracted_path}/{member.name}", "rb") as binary_file:
                    storage.store_binary(kbid, member.name, binary_file.read())


@api.get("/export/{kbid}")
async def export_endpoint(kbid: str):
    return StreamingResponse(
        stream_from_tarfile(kbid),
        status_code=200,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename={kbid}.resources.export"
        },
    )


@api.post("/import/{kbid}")
async def import_endpoint(request: Request, kbid: str):
    with tempfile.TemporaryDirectory() as tempfolder:
        path = await stream_to_tarfile(request, kbid, tempfolder)
        import_tarfile(kbid, tempfolder, path)


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
